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

/// Todos anyone reads unless they have an owner: then their owner, or
/// whoever holds a grant.
fn schema() -> JazzSchema {
    use jazz::tools::{CmpOp, PolicyExpr, PolicyValue, TablePolicies};
    let me = || {
        PolicyValue::SessionRef(vec![
            "user".to_owned(),
            "identity".to_owned(),
            "subject".to_owned(),
        ])
    };
    let readable = PolicyExpr::Or(vec![
        PolicyExpr::IsNull {
            column: "owner".to_owned(),
        },
        PolicyExpr::Cmp {
            column: "owner".to_owned(),
            op: CmpOp::Eq,
            value: me(),
        },
        PolicyExpr::Exists {
            table: "grants".to_owned(),
            condition: Box::new(PolicyExpr::Cmp {
                column: "grantee".to_owned(),
                op: CmpOp::Eq,
                value: me(),
            }),
        },
    ]);
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("grants")
                    .column("grantee", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .column("rank", ColumnType::Integer)
                    .nullable_column("owner", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(
                        TablePolicies::new()
                            .with_select(readable)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
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

/// A duplex that counts the row images the server ships, and its
/// evictions.
struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    shipped: Option<Rc<RefCell<usize>>>,
}

thread_local! {
    static EVICTIONS: RefCell<usize> = const { RefCell::new(0) };
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if let (Some(count), SyncMessage::Rows { images, .. }) = (&self.shipped, &message) {
            *count.borrow_mut() += images.len();
        }
        if let (Some(_), SyncMessage::AccessLost { .. }) = (&self.shipped, &message) {
            EVICTIONS.with(|count| *count.borrow_mut() += 1);
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
    server.accept_subscriber(server_side, alice()).unwrap();
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
fn a_tab_keeps_a_windowed_view_over_a_frozen_base() {
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
    let (main, draft, other) = (branch(0x91), branch(0x92), branch(0x93));
    put(&server, &main, 0xa0, "a", 1);
    put(&server, &main, 0xa1, "b", 2);
    put(&server, &main, 0xa2, "c", 3);
    put(&server, &main, 0xa3, "d", 4);
    // Rows on another branch that a window over every branch would take.
    put(&server, &other, 0xa4, "other 1", -2);
    put(&server, &other, 0xa5, "other 2", -1);
    let all = tab.prepare_query(&Query::from("todos")).unwrap();
    let main_all =
        block_on(tab.subscribe(&all, ReadOpts::default().branch_view(main.clone(), None))).unwrap();
    for _ in 0..6 {
        tick();
    }
    drop(main_all);
    // The capture waits on the worker: tick while polling it.
    let mut capture = pin!(tab.capture_snapshot());
    let mut context = Context::from_waker(Waker::noop());
    let frozen = loop {
        if let Poll::Ready(frozen) = capture.as_mut().poll(&mut context) {
            break frozen.unwrap();
        }
        tick();
    };
    del(&server, 0xa0, &draft, Some(&main));
    let view = ReadOpts::default().branch_view(
        draft,
        Some(BranchViewBase::Snapshot {
            branch: main,
            snapshot: frozen,
        }),
    );
    let prepared = tab.prepare_query(&first_three()).unwrap();
    let _s = block_on(tab.subscribe(&prepared, view.clone())).unwrap();
    for _ in 0..6 {
        tick();
    }
    assert_eq!(titles(&tab, &first_three(), view), ["b", "c", "d"]);
}

fn set_rank(db: &Db, row: u8, head: &BranchSelector, base: &BranchSelector, rank: i32) {
    block_on(db.update(
        "todos",
        RowUuid::from_bytes([row; 16]),
        BTreeMap::from([("rank".to_owned(), Value::I32(rank))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(base.clone())),
            },
            ..Default::default()
        },
    ))
    .unwrap();
}

fn rank_one() -> Query {
    Query::from("todos")
        .filter(jazz::query::eq(
            jazz::query::col("rank"),
            jazz::query::lit(Value::I32(1)),
        ))
        .order_by("title", OrderDirection::Asc)
}

/// A replica that synced main's x and y (both rank 1, written elsewhere)
/// through a read it has since closed, and a view of `draft` over main
/// frozen at that point.
fn frozen_base(main: u8, draft: u8) -> (Db, Db, BranchSelector, BranchSelector, ReadOpts) {
    let (server, client, _) = connected();
    let (main, draft) = (branch(main), branch(draft));
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 1);
    let all = client.prepare_query(&Query::from("todos")).unwrap();
    let main_all =
        block_on(client.subscribe(&all, ReadOpts::default().branch_view(main.clone(), None)))
            .unwrap();
    pump(&server, &client);
    drop(main_all);
    pump(&server, &client);
    let frozen = block_on(client.capture_snapshot()).unwrap();
    let view = ReadOpts::default().branch_view(
        draft.clone(),
        Some(BranchViewBase::Snapshot {
            branch: main.clone(),
            snapshot: frozen,
        }),
    );
    (server, client, main, draft, view)
}

#[test]
fn a_frozen_base_view_drops_a_row_another_writer_moves_out_of_its_filter() {
    let (server, client, main, draft, view) = frozen_base(0xc1, 0xc2);
    let prepared = client.prepare_query(&rank_one()).unwrap();
    let _s = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &rank_one(), view.clone()), ["x", "y"]);

    // The head's x no longer matches, but it still masks the frozen x.
    set_rank(&server, 0x45, &draft, &main, 9);
    pump(&server, &client);
    assert_eq!(titles(&client, &rank_one(), view), ["y"]);
}

#[test]
fn a_frozen_base_view_opens_without_a_row_its_head_moved_out_of_the_filter() {
    let (server, client, main, draft, view) = frozen_base(0xc5, 0xc6);
    set_rank(&server, 0x45, &draft, &main, 9);
    pump(&server, &client);
    let prepared = client.prepare_query(&rank_one()).unwrap();
    let _s = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &rank_one(), view), ["y"]);
}

#[test]
fn a_filtered_frozen_base_view_follows_a_head_tombstone() {
    let (server, client, main, draft, view) = frozen_base(0xc3, 0xc4);
    let prepared = client.prepare_query(&rank_one()).unwrap();
    let _s = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    del(&server, 0x45, &draft, Some(&main));
    pump(&server, &client);
    assert_eq!(titles(&client, &rank_one(), view), ["y"]);
}

#[test]
fn a_live_base_view_masks_a_held_base_row_with_a_head_version_outside_its_filter() {
    let (server, client, _) = connected();
    let (main, draft) = (branch(0xc7), branch(0xc8));
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 1);
    set_rank(&server, 0x45, &draft, &main, 9);
    // Another read brings main's x (rank 1) to the replica.
    let all = Query::from("todos").order_by("title", OrderDirection::Asc);
    let main_all = client.prepare_query(&all).unwrap();
    let _m = block_on(client.subscribe(
        &main_all,
        ReadOpts::default().branch_view(main.clone(), None),
    ))
    .unwrap();
    pump(&server, &client);

    let view = ReadOpts::default().branch_view(draft, Some(BranchViewBase::Current(main)));
    let prepared = client.prepare_query(&rank_one()).unwrap();
    let _s = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &rank_one(), view), ["y"]);
}

#[test]
fn a_head_tombstone_masks_a_base_row_that_arrives_after_the_view_opened() {
    let (server, client, _) = connected();
    let (main, draft) = (branch(0xf9), branch(0xfa));
    put(&server, &main, 0x70, "x", 1);
    put(&server, &main, 0x71, "y", 2);
    put(&server, &main, 0x72, "z", 3);
    del(&server, 0x70, &draft, Some(&main));
    let query = Query::from("todos").order_by("rank", OrderDirection::Asc);
    let draft_view =
        ReadOpts::default().branch_view(draft, Some(BranchViewBase::Current(main.clone())));
    let prepared = client.prepare_query(&query).unwrap();
    let windowed = client.prepare_query(&query.clone().limit(2)).unwrap();
    let _d = block_on(client.subscribe(&prepared, draft_view.clone())).unwrap();
    let _w = block_on(client.subscribe(&windowed, draft_view.clone())).unwrap();
    pump(&server, &client);

    // A main subscription opened later brings main's x.
    let _m = block_on(client.subscribe(
        &prepared,
        ReadOpts::default().branch_view(main.clone(), None),
    ))
    .unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &query, draft_view.clone()), ["y", "z"]);
    assert_eq!(titles(&client, &query.limit(2), draft_view), ["y", "z"]);

    // So does a one-shot remote read of main.
    let (server, client, _) = connected();
    let (main, draft) = (branch(0xfb), branch(0xfc));
    put(&server, &main, 0x70, "x", 1);
    put(&server, &main, 0x71, "y", 2);
    del(&server, 0x70, &draft, Some(&main));
    let query = Query::from("todos").order_by("rank", OrderDirection::Asc);
    let draft_view =
        ReadOpts::default().branch_view(draft, Some(BranchViewBase::Current(main.clone())));
    let prepared = client.prepare_query(&query).unwrap();
    let _d = block_on(client.subscribe(&prepared, draft_view.clone())).unwrap();
    pump(&server, &client);
    let mut opts = ReadOpts::default().branch_view(main, None);
    opts.tier = ReadTier::Remote;
    let mut read = pin!(client.all(&prepared, opts));
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(result) = read.as_mut().poll(&mut cx) {
            result.unwrap();
            break;
        }
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    pump(&server, &client);
    assert_eq!(titles(&client, &query, draft_view), ["y"]);
}

/// A one-shot read through the host path, settled by the server at `tier`.
fn read_once(server: &Db, client: &Db, query: &Query, opts: ReadOpts) -> Vec<String> {
    let bytes = postcard::to_allocvec(query).unwrap();
    let mut read = pin!(client.all_serialized_query(&bytes, opts, None, None, None));
    let mut cx = Context::from_waker(Waker::noop());
    let rows = loop {
        if let Poll::Ready(result) = read.as_mut().poll(&mut cx) {
            match result.unwrap() {
                jazz::db::SerializedReadResult::Rows(rows) => break rows,
                other => panic!("{other:?}"),
            }
        }
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    };
    rows.iter()
        .map(|row| match row.cell("title") {
            Some(Value::String(title)) => title,
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn a_one_shot_view_read_masks_a_held_base_row_with_its_head() {
    let (server, client, _) = connected();
    let (main, draft) = (branch(0xd9), branch(0xda));
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 1);
    put(&server, &main, 0x47, "z", 1);
    set_rank(&server, 0x45, &draft, &main, 9);
    del(&server, 0x47, &draft, Some(&main));
    // A main subscription brings main's x and z.
    let all = client.prepare_query(&Query::from("todos")).unwrap();
    let _m = block_on(client.subscribe(&all, ReadOpts::default().branch_view(main.clone(), None)))
        .unwrap();
    pump(&server, &client);

    let mut opts = ReadOpts::default().branch_view(draft, Some(BranchViewBase::Current(main)));
    opts.tier = ReadTier::Remote;
    assert_eq!(read_once(&server, &client, &rank_one(), opts), ["y"]);
}

/// A replica holds main's x and y (rank 1) through a main subscription,
/// and captures main; while it is away, the draft deletes x (`tombstone`)
/// or moves it out of the filter. Back online, it reads the draft over
/// main: frozen at its capture, or live.
fn reconnect_then_view(
    tombstone: bool,
    frozen_base: bool,
    one_shot: bool,
    bytes: (u8, u8),
) -> Vec<String> {
    let server = open(0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    let client = open(0x02, alice());
    let (up, _) = connect(&server, &client);
    let (main, draft) = (branch(bytes.0), branch(bytes.1));
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 1);
    let all = client.prepare_query(&Query::from("todos")).unwrap();
    let _m = block_on(client.subscribe(&all, ReadOpts::default().branch_view(main.clone(), None)))
        .unwrap();
    pump(&server, &client);
    let frozen = block_on(client.capture_snapshot()).unwrap();
    assert!(client.detach_connection(&up));
    if tombstone {
        del(&server, 0x45, &draft, Some(&main));
    } else {
        set_rank(&server, 0x45, &draft, &main, 9);
    }
    std::mem::forget(connect(&server, &client).0);
    pump(&server, &client);
    let base = if frozen_base {
        BranchViewBase::Snapshot {
            branch: main,
            snapshot: frozen,
        }
    } else {
        BranchViewBase::Current(main)
    };
    let mut view = ReadOpts::default().branch_view(draft, Some(base));
    if one_shot {
        view.tier = ReadTier::Remote;
        return read_once(&server, &client, &rank_one(), view);
    }
    let prepared = client.prepare_query(&rank_one()).unwrap();
    let _s = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    titles(&client, &rank_one(), view)
}

#[test]
fn a_view_opened_after_a_reconnect_masks_what_the_head_changed_while_away() {
    let mut bytes = 0x10;
    for tombstone in [true, false] {
        for frozen_base in [true, false] {
            for one_shot in [true, false] {
                bytes += 2;
                assert_eq!(
                    reconnect_then_view(tombstone, frozen_base, one_shot, (bytes, bytes + 1)),
                    ["y"],
                    "tombstone {tombstone}, frozen base {frozen_base}, one-shot {one_shot}"
                );
            }
        }
    }
}

fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xb0; 16])
}

/// `who`'s subject, as an `owner` or a `grantee`.
fn subject(who: AuthorSubject) -> Value {
    Value::String(who.principal_parts().1)
}

/// Write `cells` to row `row` in the draft `head` over a live `base`.
fn set(db: &Db, row: u8, head: &BranchSelector, base: &BranchSelector, cells: &[(&str, Value)]) {
    block_on(
        db.update(
            "todos",
            RowUuid::from_bytes([row; 16]),
            cells
                .iter()
                .map(|(column, value)| ((*column).to_owned(), value.clone()))
                .collect(),
            jazz::db::UpdateOptions {
                target: jazz::db::WriteTarget::BranchView {
                    head: head.clone(),
                    base: Some(BranchViewBase::Current(base.clone())),
                },
                ..Default::default()
            },
        ),
    )
    .unwrap();
}

/// Grant `who` every todo, as grant row 0x99.
fn grant(db: &Db, who: AuthorSubject) {
    block_on(db.insert(
        "grants",
        BTreeMap::from([("grantee".to_owned(), subject(who))]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([0x99; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn by_rank() -> Query {
    Query::from("todos").order_by("rank", OrderDirection::Asc)
}

/// The draft moves x to an owner Alice may not read: in her draft view x
/// is hidden (not main's x, which the draft overrides), while her main
/// view keeps main's x. The draft's version reaching her again, by a
/// write or by a grant, lifts the marker.
#[test]
fn a_head_version_the_reader_may_not_read_hides_the_row_in_a_live_view() {
    let (server, client, _) = connected();
    let (main, draft) = (branch(0x31), branch(0x32));
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 2);
    let main_view = ReadOpts::default().branch_view(main.clone(), None);
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let all = client.prepare_query(&by_rank()).unwrap();
    let _m = block_on(client.subscribe(&all, main_view.clone())).unwrap();
    let _v = block_on(client.subscribe(&all, view.clone())).unwrap();
    pump(&server, &client);
    set(
        &server,
        0x45,
        &draft,
        &main,
        &[("title", Value::String("x draft".into()))],
    );
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), view.clone()), ["x draft", "y"]);

    set(&server, 0x45, &draft, &main, &[("owner", subject(bob()))]);
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), view.clone()), ["y"]);
    assert_eq!(titles(&client, &by_rank(), main_view.clone()), ["x", "y"]);
    // Deleted rows of the view do not list it either.
    let mut deleted = view.clone();
    deleted.read_view.include_deleted = true;
    assert_eq!(titles(&client, &by_rank(), deleted), ["y"]);

    // Moved back: its image lifts the marker.
    set(
        &server,
        0x45,
        &draft,
        &main,
        &[("owner", Value::Nullable(None))],
    );
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), view.clone()), ["x draft", "y"]);

    // Hidden again, then a grant lets Alice read it.
    set(&server, 0x45, &draft, &main, &[("owner", subject(bob()))]);
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), view.clone()), ["y"]);
    grant(&server, alice());
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), view), ["x draft", "y"]);
    assert_eq!(titles(&client, &by_rank(), main_view), ["x", "y"]);
}

/// The draft already hid x from Alice when her view opens: before it, or
/// while she was away. Live or one-shot, over main live or frozen at her
/// capture, her view hides x though she holds main's x.
fn hidden_before_the_view(
    away: bool,
    frozen_base: bool,
    one_shot: bool,
    bytes: (u8, u8),
) -> Vec<String> {
    let server = open(0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    let client = open(0x02, alice());
    let (up, _) = connect(&server, &client);
    let (main, draft) = (branch(bytes.0), branch(bytes.1));
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 2);
    let all = client.prepare_query(&by_rank()).unwrap();
    let _m = block_on(client.subscribe(&all, ReadOpts::default().branch_view(main.clone(), None)))
        .unwrap();
    pump(&server, &client);
    let frozen = block_on(client.capture_snapshot()).unwrap();
    if away {
        assert!(client.detach_connection(&up));
    } else {
        std::mem::forget(up);
    }
    set(&server, 0x45, &draft, &main, &[("owner", subject(bob()))]);
    if away {
        std::mem::forget(connect(&server, &client).0);
    }
    pump(&server, &client);
    let base = if frozen_base {
        BranchViewBase::Snapshot {
            branch: main,
            snapshot: frozen,
        }
    } else {
        BranchViewBase::Current(main)
    };
    let mut view = ReadOpts::default().branch_view(draft, Some(base));
    if one_shot {
        view.tier = ReadTier::Remote;
        return read_once(&server, &client, &by_rank(), view);
    }
    let _v = block_on(client.subscribe(&all, view.clone())).unwrap();
    pump(&server, &client);
    titles(&client, &by_rank(), view)
}

#[test]
fn a_view_opened_after_the_head_hid_a_row_hides_it() {
    let mut bytes = 0x40;
    for away in [false, true] {
        for frozen_base in [true, false] {
            for one_shot in [true, false] {
                bytes += 2;
                assert_eq!(
                    hidden_before_the_view(away, frozen_base, one_shot, (bytes, bytes + 1)),
                    ["y"],
                    "away {away}, frozen base {frozen_base}, one-shot {one_shot}"
                );
            }
        }
    }
}

/// A tab reads through its worker: the worker passes its marker on, and
/// lifts it when the grant brings the draft's version.
#[test]
fn a_tab_hides_a_row_its_worker_may_not_read_on_the_head() {
    let server = open(0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    let worker = open(0x02, alice());
    std::mem::forget(connect(&server, &worker).0);
    let tab = open(0x03, alice());
    tab.set_non_durable_client();
    std::mem::forget(connect(&worker, &tab).0);
    let tick = || {
        for _ in 0..6 {
            block_on(tab.tick()).unwrap();
            block_on(worker.tick()).unwrap();
            block_on(server.tick()).unwrap();
            block_on(worker.tick()).unwrap();
            block_on(tab.tick()).unwrap();
        }
    };
    let (main, draft) = (branch(0x61), branch(0x62));
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 2);
    set(&server, 0x45, &draft, &main, &[("owner", subject(bob()))]);
    let main_view = ReadOpts::default().branch_view(main.clone(), None);
    let view = ReadOpts::default().branch_view(draft, Some(BranchViewBase::Current(main)));
    let all = tab.prepare_query(&by_rank()).unwrap();
    let _m = block_on(tab.subscribe(&all, main_view.clone())).unwrap();
    tick();
    let _v = block_on(tab.subscribe(&all, view.clone())).unwrap();
    tick();
    assert_eq!(titles(&tab, &by_rank(), main_view.clone()), ["x", "y"]);
    assert_eq!(titles(&tab, &by_rank(), view.clone()), ["y"]);
    // A one-shot read through the worker hides it too.
    let mut once = view.clone();
    once.tier = ReadTier::Remote;
    let bytes = postcard::to_allocvec(&by_rank()).unwrap();
    let mut read = pin!(tab.all_serialized_query(&bytes, once, None, None, None));
    let mut cx = Context::from_waker(Waker::noop());
    let rows = loop {
        if let Poll::Ready(result) = read.as_mut().poll(&mut cx) {
            match result.unwrap() {
                jazz::db::SerializedReadResult::Rows(rows) => break rows,
                other => panic!("{other:?}"),
            }
        }
        tick();
    };
    assert_eq!(rows.len(), 1);
    grant(&server, alice());
    tick();
    assert_eq!(titles(&tab, &by_rank(), view), ["x", "y"]);
}

/// Whether Alice may read the draft's x changes while she is away, by a
/// grant given or taken, not by a write to x: her resumed view follows.
#[test]
fn a_resumed_view_follows_access_to_a_head_row_changed_while_away() {
    for granted_while_away in [true, false] {
        let server = open(0x01, AuthorSubject::SYSTEM);
        server.set_authority();
        let client = open(0x02, alice());
        let (up, _) = connect(&server, &client);
        let (main, draft) = (branch(0x71), branch(0x72));
        put(&server, &main, 0x45, "x", 1);
        put(&server, &main, 0x46, "y", 2);
        set(
            &server,
            0x45,
            &draft,
            &main,
            &[
                ("title", Value::String("x draft".into())),
                ("owner", subject(bob())),
            ],
        );
        if !granted_while_away {
            grant(&server, alice());
        }
        let main_view = ReadOpts::default().branch_view(main.clone(), None);
        let view = ReadOpts::default().branch_view(draft, Some(BranchViewBase::Current(main)));
        let all = client.prepare_query(&by_rank()).unwrap();
        let _m = block_on(client.subscribe(&all, main_view)).unwrap();
        let _v = block_on(client.subscribe(&all, view.clone())).unwrap();
        pump(&server, &client);
        let before: &[&str] = if granted_while_away {
            &["y"]
        } else {
            &["x draft", "y"]
        };
        assert_eq!(titles(&client, &by_rank(), view.clone()), before);

        assert!(client.detach_connection(&up));
        if granted_while_away {
            grant(&server, alice());
        } else {
            block_on(server.delete(
                "grants",
                RowUuid::from_bytes([0x99; 16]),
                Default::default(),
            ))
            .unwrap();
        }
        std::mem::forget(connect(&server, &client).0);
        pump(&server, &client);
        let after: &[&str] = if granted_while_away {
            &["x draft", "y"]
        } else {
            &["y"]
        };
        assert_eq!(
            titles(&client, &by_rank(), view),
            after,
            "granted while away {granted_while_away}"
        );
    }
}

/// The marker is stored: a reopened store's view hides the row offline.
#[test]
fn a_reopened_store_keeps_hiding_a_row_offline() {
    let dir = std::env::temp_dir().join(format!("jazz-branch-view-sync-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("store.sqlite");
    let open_file = || {
        block_on(Db::open_at(
            DbConfig::new(
                schema(),
                DbIdentity {
                    node: NodeUuid::from_bytes([0x02; 16]),
                    author: alice(),
                },
            ),
            &path,
            jazz::db::FileOptions::default(),
        ))
        .unwrap()
    };
    let (main, draft) = (branch(0x81), branch(0x82));
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    {
        let server = open(0x01, AuthorSubject::SYSTEM);
        server.set_authority();
        let client = open_file();
        std::mem::forget(connect(&server, &client).0);
        put(&server, &main, 0x45, "x", 1);
        put(&server, &main, 0x46, "y", 2);
        set(&server, 0x45, &draft, &main, &[("owner", subject(bob()))]);
        let all = client.prepare_query(&by_rank()).unwrap();
        let _m =
            block_on(client.subscribe(&all, ReadOpts::default().branch_view(main, None))).unwrap();
        let _v = block_on(client.subscribe(&all, view.clone())).unwrap();
        pump(&server, &client);
        assert_eq!(titles(&client, &by_rank(), view.clone()), ["y"]);
    }
    let client = open_file();
    assert_eq!(titles(&client, &by_rank(), view), ["y"]);
    let _ = std::fs::remove_dir_all(&dir);
}

fn open_with(schema: JazzSchema, node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

/// Bob's x on main and on the draft: Alice may read neither. With her view
/// of the draft over main open, Bob's writes to x send her nothing, as
/// without it: she learns of no row she may not see through the view.
#[test]
fn writes_to_a_row_the_reader_sees_on_neither_branch_send_her_nothing() {
    let (server, client, shipped) = connected();
    let (main, draft) = (branch(0x91), branch(0x92));
    put(&server, &main, 0x10, "bob main", 1);
    set(&server, 0x10, &draft, &main, &[("owner", subject(bob()))]);
    block_on(server.update(
        "todos",
        RowUuid::from_bytes([0x10; 16]),
        BTreeMap::from([("owner".to_owned(), subject(bob()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: main.clone(),
                base: None,
            },
            ..Default::default()
        },
    ))
    .unwrap();
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let all = client.prepare_query(&by_rank()).unwrap();
    let _v = block_on(client.subscribe(&all, view.clone())).unwrap();
    pump(&server, &client);
    let (images, evictions) = (*shipped.borrow(), EVICTIONS.with(|count| *count.borrow()));
    set(
        &server,
        0x10,
        &draft,
        &main,
        &[("title", Value::String("bob draft".into()))],
    );
    pump(&server, &client);
    set_rank_on(&server, 0x10, &main, 7);
    pump(&server, &client);
    assert_eq!(*shipped.borrow(), images);
    assert_eq!(EVICTIONS.with(|count| *count.borrow()), evictions);
    assert!(titles(&client, &by_rank(), view).is_empty());
}

/// Head rows whose base row the view's query leaves out are not shipped,
/// whoever writes them.
#[test]
fn rows_outside_the_views_query_on_the_base_ship_no_head_rows() {
    let (server, client, shipped) = connected();
    let (main, draft) = (branch(0x93), branch(0x94));
    for i in 0..20u8 {
        put(&server, &main, 0x80 + i, &format!("m{i}"), 50);
    }
    put(&server, &main, 0x46, "y", 1);
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let prepared = client.prepare_query(&rank_one()).unwrap();
    let _s = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    let before = *shipped.borrow();
    for i in 0..20u8 {
        set_rank(&server, 0x80 + i, &draft, &main, 60);
    }
    pump(&server, &client);
    for i in 0..20u8 {
        set_rank_on(&server, 0x80 + i, &main, 70);
    }
    pump(&server, &client);
    assert_eq!(*shipped.borrow(), before);
    assert_eq!(titles(&client, &by_rank(), view), ["y"]);
}

fn set_rank_on(db: &Db, row: u8, on: &BranchSelector, rank: i32) {
    block_on(db.update(
        "todos",
        RowUuid::from_bytes([row; 16]),
        BTreeMap::from([("rank".to_owned(), Value::I32(rank))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: on.clone(),
                base: None,
            },
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Resumed live view: the head's x (tombstone or outside the filter) is
/// older than `since`; while the replica is away main's x ENTERS the
/// filter, and the main subscription brings it back. The resumed opening
/// ships only head rows written after `since`.
fn resume_base_enters_filter(tombstone: bool, bytes: (u8, u8)) -> Vec<String> {
    let server = open(0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    let client = open(0x02, alice());
    let (up, _) = connect(&server, &client);
    let (main, draft) = (branch(bytes.0), branch(bytes.1));
    put(&server, &main, 0x45, "x", 5);
    put(&server, &main, 0x46, "y", 1);
    if tombstone {
        del(&server, 0x45, &draft, Some(&main));
    } else {
        set_rank(&server, 0x45, &draft, &main, 9);
    }
    let all = client.prepare_query(&Query::from("todos")).unwrap();
    let _m = block_on(client.subscribe(&all, ReadOpts::default().branch_view(main.clone(), None)))
        .unwrap();
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let prepared = client.prepare_query(&rank_one()).unwrap();
    let _s = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &rank_one(), view.clone()), ["y"]);
    assert!(client.detach_connection(&up));
    set_rank_on(&server, 0x45, &main, 1);
    std::mem::forget(connect(&server, &client).0);
    pump(&server, &client);
    pump(&server, &client);
    titles(&client, &rank_one(), view)
}

#[test]
fn a_resumed_view_masks_a_base_row_that_entered_its_filter_while_away() {
    assert_eq!(resume_base_enters_filter(true, (0x51, 0x52)), ["y"]);
    assert_eq!(resume_base_enters_filter(false, (0x53, 0x54)), ["y"]);
}

fn inc_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("lists")
                    .column("branch_id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .fk_column("list", "lists")
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// A relation of a branched table: the draft deletes or renames the list;
/// the replica's view shows what the server's does, whether it holds
/// main's list through a main lists read or not.
fn relation_names(tombstone: bool, bytes: (u8, u8), main_sub: bool) -> (String, String) {
    let server = open_with(inc_schema(), 0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    let client = open_with(inc_schema(), 0x02, alice());
    std::mem::forget(connect(&server, &client).0);
    let (main, draft) = (branch(bytes.0), branch(bytes.1));
    let l = RowUuid::from_bytes([0x30; 16]);
    block_on(server.insert(
        "lists",
        BTreeMap::from([("name".to_owned(), Value::String("main list".into()))]),
        InsertOptions {
            row_id: Some(l),
            target: ExactWriteTarget::Branch(main.clone()),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(server.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("t".into())),
            (
                "list".to_owned(),
                Value::Uuid(uuid::Uuid::from_bytes([0x30; 16])),
            ),
        ]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([0x31; 16])),
            target: ExactWriteTarget::Branch(main.clone()),
            ..Default::default()
        },
    ))
    .unwrap();
    let target = jazz::db::WriteTarget::BranchView {
        head: draft.clone(),
        base: Some(BranchViewBase::Current(main.clone())),
    };
    if tombstone {
        block_on(server.delete(
            "lists",
            l,
            jazz::db::DeleteOptions {
                target,
                ..Default::default()
            },
        ))
        .unwrap();
    } else {
        block_on(server.update(
            "lists",
            l,
            BTreeMap::from([("name".to_owned(), Value::String("draft list".into()))]),
            jazz::db::UpdateOptions {
                target,
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let lists = client.prepare_query(&Query::from("lists")).unwrap();
    let _m = main_sub.then(|| {
        block_on(client.subscribe(&lists, ReadOpts::default().branch_view(main.clone(), None)))
            .unwrap()
    });
    pump(&server, &client);
    let q = Query::from("todos").array_subquery(jazz::query::ArraySubquery::new(
        "lst", "lists", "id", "list",
    ));
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let p = client.prepare_query(&q).unwrap();
    let _s = block_on(client.subscribe(&p, view.clone())).unwrap();
    pump(&server, &client);
    let mut local = view.clone();
    local.tier = ReadTier::LocalOnly;
    let names = |t: jazz::result_tree::ResultTree| -> Vec<String> {
        t.roots
            .iter()
            .map(|n| {
                format!(
                    "{:?}",
                    n.relations.get("lst").map(|r| match r {
                        jazz::result_tree::ResultRelation::Array(ch) => ch
                            .iter()
                            .map(|c| format!("{:?}", c.row.cell("name")))
                            .collect::<Vec<_>>(),
                        o => vec![format!("{o:?}")],
                    })
                )
            })
            .collect()
    };
    let c = names(block_on(client.all_result_tree(&p, local.clone())).unwrap());
    let sp = server.prepare_query(&q).unwrap();
    let s = names(block_on(server.all_result_tree(&sp, local)).unwrap());
    (c.join(","), s.join(","))
}

#[test]
fn a_views_relations_read_the_heads_rows_over_held_base_rows() {
    let mut bytes = 0xa0;
    for tombstone in [true, false] {
        for main_sub in [true, false] {
            bytes += 2;
            let (client, server) = relation_names(tombstone, (bytes, bytes + 1), main_sub);
            assert_eq!(
                client, server,
                "tombstone {tombstone}, main read {main_sub}"
            );
        }
    }
}

/// The ids `path`'s `_jazz_withheld` marks, or `None` before any marker
/// created the table.
fn withheld_ids(path: &std::path::Path) -> Option<Vec<u8>> {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let exists: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name = '_jazz_withheld'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    if exists == 0 {
        return None;
    }
    let mut statement = conn.prepare("SELECT id FROM _jazz_withheld").unwrap();
    Some(
        statement
            .query_map([], |r| r.get::<_, Vec<u8>>(0))
            .unwrap()
            .map(|id| id.unwrap()[0])
            .collect(),
    )
}

fn file_client(tag: &str) -> (Db, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "jazz-branch-view-sync-{tag}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("store.sqlite");
    let client = block_on(Db::open_at(
        DbConfig::new(
            schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([0x02; 16]),
                author: alice(),
            },
        ),
        &path,
        jazz::db::FileOptions::default(),
    ))
    .unwrap();
    (client, path)
}

/// Once a row is hidden, Bob's further edits of the head version, and
/// edits of the base row under it, send Alice nothing: she learns that
/// the row is hidden, not when Bob edits it.
#[test]
fn edits_to_a_hidden_head_row_send_nothing_more() {
    let (server, client, shipped) = connected();
    let (main, draft) = (branch(0xa1), branch(0xa2));
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 2);
    set(&server, 0x45, &draft, &main, &[("owner", subject(bob()))]);
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let all = client.prepare_query(&by_rank()).unwrap();
    let _v = block_on(client.subscribe(&all, view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), view.clone()), ["y"]);
    let (images, evictions) = (*shipped.borrow(), EVICTIONS.with(|count| *count.borrow()));
    for i in 0..3 {
        set(
            &server,
            0x45,
            &draft,
            &main,
            &[("title", Value::String(format!("bob {i}")))],
        );
        pump(&server, &client);
    }
    set_rank_on(&server, 0x45, &main, 5);
    pump(&server, &client);
    assert_eq!(EVICTIONS.with(|count| *count.borrow()), evictions);
    assert_eq!(*shipped.borrow(), images);
    assert_eq!(titles(&client, &by_rank(), view.clone()), ["y"]);
    // A grant still un-hides it: the row's access changed.
    grant(&server, alice());
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), view), ["bob 2", "y"]);
}

/// Access lost on a read with no branch view over a base evicts the row
/// and keeps no marker: markers exist only where a view can use them.
#[test]
fn a_revoke_without_a_view_keeps_no_marker() {
    let (client, path) = file_client("plain");
    let server = open(0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    std::mem::forget(connect(&server, &client).0);
    let main = branch(0xa3);
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 2);
    let all = client.prepare_query(&by_rank()).unwrap();
    let opts = ReadOpts::default().branch_view(main.clone(), None);
    let _m = block_on(client.subscribe(&all, opts.clone())).unwrap();
    pump(&server, &client);
    block_on(server.update(
        "todos",
        RowUuid::from_bytes([0x45; 16]),
        BTreeMap::from([("owner".to_owned(), subject(bob()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: main.clone(),
                base: None,
            },
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), opts), ["y"]);
    assert_eq!(withheld_ids(&path), None);
    // The same revoke on a view's head keeps one, for the view.
    let draft = branch(0xa4);
    set(&server, 0x46, &draft, &main, &[("owner", subject(bob()))]);
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let _v = block_on(client.subscribe(&all, view.clone())).unwrap();
    pump(&server, &client);
    assert!(titles(&client, &by_rank(), view).is_empty());
    assert_eq!(withheld_ids(&path), Some(vec![0x46]));
}

/// A write through the view to a hidden row is refused: it would land
/// over a head version this replica may not read.
#[test]
fn a_write_through_a_view_to_a_hidden_row_is_refused() {
    let (server, client, _) = connected();
    let (main, draft) = (branch(0xa5), branch(0xa6));
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 2);
    set(
        &server,
        0x45,
        &draft,
        &main,
        &[
            ("title", Value::String("bob draft".into())),
            ("owner", subject(bob())),
        ],
    );
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let all = client.prepare_query(&by_rank()).unwrap();
    let _v = block_on(client.subscribe(&all, view.clone())).unwrap();
    pump(&server, &client);
    let target = jazz::db::WriteTarget::BranchView {
        head: draft.clone(),
        base: Some(BranchViewBase::Current(main.clone())),
    };
    let x = RowUuid::from_bytes([0x45; 16]);
    let update = block_on(client.update(
        "todos",
        x,
        BTreeMap::from([("rank".to_owned(), Value::I32(3))]),
        jazz::db::UpdateOptions {
            target: target.clone(),
            ..Default::default()
        },
    ));
    let delete = block_on(client.delete(
        "todos",
        x,
        jazz::db::DeleteOptions {
            target,
            ..Default::default()
        },
    ));
    for refused in [update.err(), delete.err()] {
        let refused = refused.expect("refused");
        assert_eq!(refused.code, jazz::db::ErrorCode::WriteRejected);
    }
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), view.clone()), ["y"]);
    assert_eq!(server_draft_titles(&server, &draft), ["bob draft"]);
    // Writes to the view's other rows go through.
    set(
        &client,
        0x46,
        &draft,
        &main,
        &[("title", Value::String("y draft".into()))],
    );
    assert_eq!(titles(&client, &by_rank(), view), ["y draft"]);
}

fn server_draft_titles(server: &Db, draft: &BranchSelector) -> Vec<String> {
    titles(
        server,
        &by_rank(),
        ReadOpts::default().branch_view(draft.clone(), None),
    )
}

/// A dropped view stops following its masking rows: a later grant ships
/// none of them.
#[test]
fn a_dropped_view_stops_following_its_masking_rows() {
    let (server, client, shipped) = connected();
    let (main, draft) = (branch(0xa7), branch(0xa8));
    for i in 0..4u8 {
        put(&server, &main, 0x50 + i, &format!("m{i}"), 1);
        set(
            &server,
            0x50 + i,
            &draft,
            &main,
            &[("owner", subject(bob()))],
        );
    }
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let all = client.prepare_query(&by_rank()).unwrap();
    let v = block_on(client.subscribe(&all, view)).unwrap();
    pump(&server, &client);
    drop(v);
    pump(&server, &client);
    let images = *shipped.borrow();
    grant(&server, alice());
    pump(&server, &client);
    assert_eq!(*shipped.borrow(), images);
}

/// Alice's own write through her view gives the draft's x an owner she
/// may not read: the server accepts it, and her view then hides the row,
/// as the authority's evaluation of the view does.
#[test]
fn a_readers_own_write_she_may_not_read_hides_the_row() {
    let (client, path) = file_client("own");
    let server = open(0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    std::mem::forget(connect(&server, &client).0);
    let (main, draft) = (branch(0xa9), branch(0xaa));
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 2);
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let all = client.prepare_query(&by_rank()).unwrap();
    let _v = block_on(client.subscribe(&all, view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), view.clone()), ["x", "y"]);
    set(&client, 0x45, &draft, &main, &[("owner", subject(bob()))]);
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), view.clone()), ["y"]);
    assert_eq!(
        titles(&client, &by_rank(), view),
        server_titles(&server, &draft, &main)
    );
    assert_eq!(withheld_ids(&path), Some(vec![0x45]));
}

/// The authority's evaluation of the draft over `main` for Alice.
fn server_titles(server: &Db, draft: &BranchSelector, main: &BranchSelector) -> Vec<String> {
    let opts =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let prepared = server.prepare_query(&by_rank()).unwrap();
    block_on(server.all_for_identity(&prepared, opts, alice()))
        .unwrap()
        .iter()
        .map(|row| match row.cell("title") {
            Some(Value::String(title)) => title,
            other => panic!("title {other:?}"),
        })
        .collect()
}

/// Alice read main's x once; then a view hides the draft's x from her.
/// Main's x stays current in her store, and Bob's later draft edits send
/// her nothing, however she keeps main's x (a one-shot read or her own
/// write).
#[test]
fn hiding_the_drafts_version_keeps_the_main_row_current_and_quiet() {
    for via_commit in [false, true] {
        let (server, client, shipped) = connected();
        let b = if via_commit { 0xb1 } else { 0xb3 };
        let (main, draft) = (branch(b), branch(b + 1));
        put(&server, &main, 0x45, "x", 1);
        put(&server, &main, 0x46, "y", 2);
        set(&server, 0x45, &draft, &main, &[("owner", subject(bob()))]);
        let on_main = ReadOpts::default().branch_view(main.clone(), None);
        let mut remote = on_main.clone();
        remote.tier = ReadTier::Remote;
        read_once(&server, &client, &by_rank(), remote.clone());
        let view = ReadOpts::default()
            .branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
        let all = client.prepare_query(&by_rank()).unwrap();
        let _v = block_on(client.subscribe(&all, view.clone())).unwrap();
        pump(&server, &client);
        assert_eq!(titles(&client, &by_rank(), view.clone()), ["y"]);
        set_rank_on(&server, 0x45, &main, 3);
        set(
            &server,
            0x45,
            &main,
            &main,
            &[("title", Value::String("x main v2".into()))],
        );
        pump(&server, &client);
        assert_eq!(
            titles(&client, &by_rank(), on_main.clone()),
            ["y", "x main v2"]
        );
        let evictions = EVICTIONS.with(|count| *count.borrow());
        let images = *shipped.borrow();
        for round in 0..2 {
            if via_commit {
                set(
                    &client,
                    0x45,
                    &main,
                    &main,
                    &[("title", Value::String(format!("x alice {round}")))],
                );
            } else {
                read_once(&server, &client, &by_rank(), remote.clone());
            }
            pump(&server, &client);
            set(
                &server,
                0x45,
                &draft,
                &main,
                &[("title", Value::String(format!("bob {round}")))],
            );
            pump(&server, &client);
        }
        assert_eq!(EVICTIONS.with(|count| *count.borrow()), evictions);
        assert!(*shipped.borrow() >= images);
        assert_eq!(titles(&client, &by_rank(), view), ["y"]);
    }
}

/// Without a view, Bob's draft edits Alice may not read still leave her
/// main row current: one eviction of the draft's version, then nothing.
#[test]
fn an_unreadable_draft_version_leaves_the_main_row_watched() {
    let (server, client, _) = connected();
    let (main, draft) = (branch(0xb5), branch(0xb6));
    put(&server, &main, 0x45, "x", 1);
    set(&server, 0x45, &draft, &main, &[("owner", subject(bob()))]);
    let on_main = ReadOpts::default().branch_view(main.clone(), None);
    let all = client.prepare_query(&by_rank()).unwrap();
    let _m = block_on(client.subscribe(&all, on_main.clone())).unwrap();
    pump(&server, &client);
    let evictions = EVICTIONS.with(|count| *count.borrow());
    for i in 0..3 {
        set(
            &server,
            0x45,
            &draft,
            &main,
            &[("title", Value::String(format!("bob {i}")))],
        );
        pump(&server, &client);
    }
    assert!(EVICTIONS.with(|count| *count.borrow()) <= evictions + 1);
    set(
        &server,
        0x45,
        &main,
        &main,
        &[("title", Value::String("x v2".into()))],
    );
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), on_main), ["x v2"]);
}

/// A write to the hidden head version itself (no base) is refused too.
#[test]
fn a_head_only_write_to_a_hidden_row_is_refused() {
    let (server, client, _) = connected();
    let (main, draft) = (branch(0xb7), branch(0xb8));
    put(&server, &main, 0x45, "x", 1);
    set(&server, 0x45, &draft, &main, &[("owner", subject(bob()))]);
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let all = client.prepare_query(&by_rank()).unwrap();
    let _v = block_on(client.subscribe(&all, view)).unwrap();
    pump(&server, &client);
    let upsert = block_on(client.upsert(
        "todos",
        RowUuid::from_bytes([0x45; 16]),
        BTreeMap::from([
            ("title".to_owned(), Value::String("mine".into())),
            ("rank".to_owned(), Value::I32(1)),
        ]),
        jazz::db::UpsertOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: draft.clone(),
                base: None,
            },
            ..Default::default()
        },
    ));
    assert_eq!(
        upsert.expect_err("refused").code,
        jazz::db::ErrorCode::WriteRejected
    );
}

/// An insert with a given id onto a branch whose version of that id is
/// hidden here is refused, like any other write to that version.
#[test]
fn an_insert_with_an_id_onto_a_hidden_version_is_refused() {
    let (server, client, _) = connected();
    let (main, draft) = (branch(0xb9), branch(0xba));
    put(&server, &main, 0x45, "x", 1);
    set(&server, 0x45, &draft, &main, &[("owner", subject(bob()))]);
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let all = client.prepare_query(&by_rank()).unwrap();
    let _v = block_on(client.subscribe(&all, view.clone())).unwrap();
    pump(&server, &client);
    let insert = block_on(client.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("alice insert".into())),
            ("rank".to_owned(), Value::I32(0)),
        ]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([0x45; 16])),
            target: ExactWriteTarget::Branch(draft.clone()),
            ..Default::default()
        },
    ));
    assert_eq!(
        insert.expect_err("refused").code,
        jazz::db::ErrorCode::WriteRejected
    );
    assert!(titles(&client, &by_rank(), view).is_empty());
}

/// Alice loses main's x, gets it back through a grant (shipped by her
/// subscription), drops the subscription, and loses it again: the second
/// loss still reaches her, though her own write keeps x watched.
#[test]
fn a_row_regained_through_a_subscription_is_evicted_again() {
    let (server, client, _) = connected();
    let main = branch(0xbb);
    put(&server, &main, 0x45, "x", 1);
    put(&server, &main, 0x46, "y", 2);
    let on_main = ReadOpts::default().branch_view(main.clone(), None);
    let all = client.prepare_query(&by_rank()).unwrap();
    let subscription = block_on(client.subscribe(&all, on_main.clone())).unwrap();
    pump(&server, &client);
    set(
        &client,
        0x45,
        &main,
        &main,
        &[("title", Value::String("x alice".into()))],
    );
    pump(&server, &client);
    set(&server, 0x45, &main, &main, &[("owner", subject(bob()))]);
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), on_main.clone()), ["y"]);
    grant(&server, alice());
    pump(&server, &client);
    assert_eq!(
        titles(&client, &by_rank(), on_main.clone()),
        ["x alice", "y"]
    );
    drop(subscription);
    pump(&server, &client);
    block_on(server.delete(
        "grants",
        RowUuid::from_bytes([0x99; 16]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &client);
    set(
        &server,
        0x45,
        &main,
        &main,
        &[("title", Value::String("bob secret".into()))],
    );
    pump(&server, &client);
    assert_eq!(titles(&client, &by_rank(), on_main), ["y"]);
}
