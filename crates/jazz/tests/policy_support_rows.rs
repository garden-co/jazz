//! The rows a read's policy support ships (#3779), pinned row by row on the
//! branching shapes of #4776: read policies whose correlated `EXISTS` arms
//! lead back to the same tables, reached directly and through an inherited
//! parent. Sharing the nested support across arms and skipping a key
//! already followed at a deeper depth (#4778) must ship exactly what the
//! unshared walk shipped: the replica ends up holding the rows the reader
//! can read that its result rests on, no more, and loses the ones it can no
//! longer read, on the first read, on live changes, and on reconnect.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SyncMessage, Transport,
    TransportError, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, Operation, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
    Value as PolicyLiteral,
};

use common::{compile_schema, exists, outer_eq, session_eq};

const ME: [&str; 2] = ["identity", "subject"];

fn all_writes(read: PolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_select(read)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

fn world(outer: &str) -> PolicyExpr {
    exists(
        "docs",
        vec![
            outer_eq("id", outer),
            PolicyExpr::eq_literal("visibility", PolicyLiteral::Text("world".into())),
        ],
    )
}

fn is_member(outer: &str) -> PolicyExpr {
    exists(
        "members",
        vec![outer_eq("doc_id", outer), session_eq("user_id", &ME)],
    )
}

/// #4776's smallest shape: three arms on `docs`, two on `members`, each
/// leading back to both tables; and `comments`, which inherit read from
/// their doc, so a comment rests on its doc and on everything the doc's
/// read rests on.
fn docs_schema() -> JazzSchema {
    docs_schema_inheriting(None)
}

/// [`docs_schema`] with comments inheriting at most `max_depth` hops.
fn docs_schema_inheriting(max_depth: Option<usize>) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("owner", ColumnType::Text)
                    .column("visibility", ColumnType::Text)
                    .policies(all_writes(PolicyExpr::or(vec![
                        is_member("id"),
                        exists("docs", vec![outer_eq("id", "id"), session_eq("owner", &ME)]),
                        world("id"),
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("doc_id", "docs")
                    .column("user_id", ColumnType::Text)
                    .policies(all_writes(PolicyExpr::or(vec![
                        is_member("doc_id"),
                        world("doc_id"),
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("comments")
                    .fk_column("doc", "docs")
                    .column("body", ColumnType::Text)
                    .policies(all_writes(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "doc".into(),
                        max_depth,
                    })),
            )
            .build(),
    )
}

/// One table whose three arms each consult the row's own table.
fn notes_schema() -> JazzSchema {
    let arm = |column: &str| exists("notes", vec![outer_eq("id", "id"), session_eq(column, &ME)]);
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .column("editor", ColumnType::Text)
                    .column("viewer", ColumnType::Text)
                    .policies(all_writes(PolicyExpr::or(vec![
                        arm("owner"),
                        arm("editor"),
                        arm("viewer"),
                    ]))),
            )
            .build(),
    )
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x40 + n; 16])
}

fn subject(n: u8) -> Value {
    Value::String(user(n).principal_parts().1)
}

const ADA: u8 = 0;
const BO: u8 = 1;
const CY: u8 = 2;

fn open(schema: JazzSchema, node: u8, author: AuthorSubject) -> Db {
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

fn id(kind: u8, n: u8) -> RowUuid {
    RowUuid::from_bytes([kind, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn uuid(row: RowUuid) -> Value {
    Value::Uuid(row.0)
}

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

fn seed(server: &Db, table: &str, row: RowUuid, values: BTreeMap<String, Value>) {
    jazz::db::block_on(server.insert(
        table,
        values,
        jazz::db::InsertOptions {
            row_id: Some(row),
            identity: jazz::db::WriteIdentity::Attribution(AuthorSubject::SYSTEM),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn insert(server: &Db, table: &str, row: RowUuid, values: BTreeMap<String, Value>) {
    block_on(server.insert(
        table,
        values,
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn update(server: &Db, table: &str, row: RowUuid, values: BTreeMap<String, Value>) {
    block_on(server.update(table, row, values, Default::default())).unwrap();
}

/// What the server sent the replica: each row image it shipped, and each
/// row the replica was told it lost.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Sent {
    images: Vec<(String, RowUuid)>,
    lost: Vec<(String, RowUuid)>,
}

impl Sent {
    fn take(log: &Rc<RefCell<Sent>>) -> Sent {
        let mut sent = std::mem::take(&mut *log.borrow_mut());
        sent.images.sort();
        sent.lost.sort();
        sent
    }
}

struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    log: Option<Rc<RefCell<Sent>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if let Some(log) = &self.log {
            let mut log = log.borrow_mut();
            match &message {
                SyncMessage::Rows { images, .. } => log.images.extend(
                    images
                        .iter()
                        .map(|image| (image.table.clone(), RowUuid::from_bytes(image.id))),
                ),
                SyncMessage::AccessLost { table, id, .. } => {
                    log.lost.push((table.clone(), RowUuid::from_bytes(*id)))
                }
                _ => {}
            }
        }
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// Connect `client` to `server` as `who`, logging what the server sends.
fn connect(server: &Db, client: &Db, who: AuthorSubject) -> (Upstream, Rc<RefCell<Sent>>) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let log = Rc::new(RefCell::new(Sent::default()));
    let client_side = Pipe {
        out: up.clone(),
        inbound: down.clone(),
        log: None,
    };
    let server_side = Pipe {
        out: down,
        inbound: up,
        log: Some(log.clone()),
    };
    let upstream = block_on(client.connect_upstream(Box::new(client_side)));
    server
        .accept_subscriber(Box::new(server_side), who)
        .unwrap();
    (upstream, log)
}

/// Poll `future` to completion, ticking `dbs` between polls.
fn drive<T>(dbs: &[&Db], future: impl std::future::Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..200 {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..6 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

/// The rows the replica holds, by table, with their cells.
fn held(db: &Db, tables: &[&str]) -> BTreeMap<String, BTreeMap<RowUuid, String>> {
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    tables
        .iter()
        .map(|table| {
            let prepared = db.prepare_query(&Query::from(*table)).unwrap();
            let rows = block_on(db.all(&prepared, opts.clone()))
                .unwrap()
                .iter()
                .map(|row| {
                    let mut cells: Vec<String> = row
                        .cells()
                        .map(|(name, value)| format!("{name}={value:?}"))
                        .collect();
                    cells.sort();
                    (row.row_uuid(), cells.join(","))
                })
                .collect();
            ((*table).to_owned(), rows)
        })
        .collect()
}

/// The ids of `held`, by table.
fn ids(held: &BTreeMap<String, BTreeMap<RowUuid, String>>) -> BTreeMap<String, Vec<RowUuid>> {
    held.iter()
        .map(|(table, rows)| (table.clone(), rows.keys().copied().collect()))
        .collect()
}

fn expect(tables: &[(&str, &[RowUuid])]) -> BTreeMap<String, Vec<RowUuid>> {
    tables
        .iter()
        .map(|(table, rows)| {
            let mut rows = rows.to_vec();
            rows.sort();
            ((*table).to_owned(), rows)
        })
        .collect()
}

/// Every row `held` lists is the server's current version of it.
fn assert_current(server: &Db, held: &BTreeMap<String, BTreeMap<RowUuid, String>>) {
    let tables: Vec<&str> = held.keys().map(String::as_str).collect();
    let truth = self::held(server, &tables);
    for (table, rows) in held {
        for (row, cells) in rows {
            assert_eq!(
                truth[table].get(row),
                Some(cells),
                "{table} {row:?} is current on the replica"
            );
        }
    }
}

fn distinct(rows: &[(String, RowUuid)]) -> BTreeSet<(String, RowUuid)> {
    rows.iter().cloned().collect()
}

fn rows(table: &str, ids: &[RowUuid]) -> BTreeSet<(String, RowUuid)> {
    ids.iter().map(|id| (table.to_owned(), *id)).collect()
}

/// Ada reads only `comments`. Everything else her replica holds is the
/// support of their read: each comment's doc (inherited read) and what the
/// doc's read rests on through its three arms, and through those, what
/// each member row's read rests on.
///
/// ```text
///  doc  owner visibility  members         comments  ada reads the doc as
///  d1   bo    private     ada, bo         c1        member
///  d2   ada   private     bo              c2        owner (not its members)
///  d3   bo    world       cy              —         world, but nothing rests on it
///  d4   bo    private     cy              c4        —
/// ```
#[test]
fn comments_ship_exactly_the_docs_and_members_their_read_rests_on() {
    let (d1, d2, d3, d4) = (id(1, 1), id(1, 2), id(1, 3), id(1, 4));
    let (m1_ada, m1_bo, m2_bo, m3_cy, m4_cy, m4_ada) =
        (id(2, 1), id(2, 2), id(2, 3), id(2, 4), id(2, 5), id(2, 6));
    let (c1, c2, c3, c4) = (id(3, 1), id(3, 2), id(3, 3), id(3, 4));
    let server = open(docs_schema(), 0, AuthorSubject::SYSTEM);
    for (doc, owner, visibility) in [
        (d1, BO, "private"),
        (d2, ADA, "private"),
        (d3, BO, "world"),
        (d4, BO, "private"),
    ] {
        seed(
            &server,
            "docs",
            doc,
            cells(&[("owner", subject(owner)), ("visibility", visibility.into())]),
        );
    }
    for (member, doc, who) in [
        (m1_ada, d1, ADA),
        (m1_bo, d1, BO),
        (m2_bo, d2, BO),
        (m3_cy, d3, CY),
        (m4_cy, d4, CY),
    ] {
        seed(
            &server,
            "members",
            member,
            cells(&[("doc_id", uuid(doc)), ("user_id", subject(who))]),
        );
    }
    for (comment, doc) in [(c1, d1), (c2, d2), (c4, d4)] {
        seed(
            &server,
            "comments",
            comment,
            cells(&[("doc", uuid(doc)), ("body", "c".into())]),
        );
    }
    let tables = ["comments", "docs", "members"];

    let ada = open(docs_schema(), 1, user(ADA));
    let (link, log) = connect(&server, &ada, user(ADA));
    let comments = ada.prepare_query(&Query::from("comments")).unwrap();
    let _live = block_on(ada.subscribe(&comments, ReadOpts::default())).unwrap();
    pump(&server, &ada);
    let first = expect(&[
        ("comments", &[c1, c2]),
        ("docs", &[d1, d2]),
        // Not d2's members: Ada reads d2 as its owner, and can't read them.
        ("members", &[m1_ada, m1_bo]),
    ]);
    let now = held(&ada, &tables);
    assert_eq!(ids(&now), first, "the first read");
    assert_current(&server, &now);
    let sent = Sent::take(&log);
    assert_eq!(
        distinct(&sent.images),
        &(&rows("comments", &[c1, c2]) | &rows("docs", &[d1, d2]))
            | &rows("members", &[m1_ada, m1_bo]),
        "the first read ships the result and its support, nothing else: {sent:?}"
    );

    // Live: Ada joins d4, so c4 enters her result with its support; d2
    // turns world, so d2's members become readable support of c2.
    insert(
        &server,
        "members",
        m4_ada,
        cells(&[("doc_id", uuid(d4)), ("user_id", subject(ADA))]),
    );
    update(
        &server,
        "docs",
        d2,
        cells(&[("visibility", "world".into())]),
    );
    pump(&server, &ada);
    let live = expect(&[
        ("comments", &[c1, c2, c4]),
        ("docs", &[d1, d2, d4]),
        ("members", &[m1_ada, m1_bo, m2_bo, m4_cy, m4_ada]),
    ]);
    let now = held(&ada, &tables);
    assert_eq!(ids(&now), live, "after joining d4 and d2 turning world");
    assert_current(&server, &now);
    let sent = Sent::take(&log);
    assert!(sent.lost.is_empty(), "nothing is lost: {sent:?}");

    // Offline: Ada leaves d1, and a comment is posted on d3 (world).
    assert!(ada.detach_connection(&link));
    block_on(server.delete("members", m1_ada, Default::default())).unwrap();
    insert(
        &server,
        "comments",
        c3,
        cells(&[("doc", uuid(d3)), ("body", "c".into())]),
    );
    let (_link, log) = connect(&server, &ada, user(ADA));
    pump(&server, &ada);
    let sent = Sent::take(&log);
    let now = held(&ada, &tables);
    assert_eq!(
        ids(&now)["comments"],
        [c2, c3, c4],
        "after reconnecting, c1 left the result and c3 entered it"
    );
    // The support c3 rests on arrives with it, and nothing else ships.
    assert_eq!(
        distinct(&sent.images),
        &(&rows("comments", &[c3]) | &rows("docs", &[d3])) | &rows("members", &[m3_cy]),
        "{sent:?}"
    );
    // c1 left the result (its listing drops it), and the support it alone
    // rested on goes with it (#5097): d1, Bo's membership and Ada's deleted
    // one, none of which she may read now, are each evicted.
    assert_eq!(
        distinct(&sent.lost),
        &rows("docs", &[d1]) | &rows("members", &[m1_ada, m1_bo]),
        "the reconnect evicts what Ada lost while offline: {sent:?}"
    );
    let resumed = expect(&[
        ("comments", &[c2, c3, c4]),
        ("docs", &[d2, d3, d4]),
        ("members", &[m2_bo, m3_cy, m4_cy, m4_ada]),
    ]);
    assert_eq!(ids(&now), resumed, "after reconnecting");
    assert_current(&server, &now);
    // Her stale membership no longer grants her d1 locally.
    let docs = ada.prepare_query(&Query::from("docs")).unwrap();
    let local = block_on(ada.all(
        &docs,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    assert!(
        local.iter().all(|row| row.row_uuid() != d1),
        "a local read of docs leaves out d1"
    );
}

/// The other side of #5097: support the read stops holding across a
/// reconnect, which the reader may still read, stays. While Ada is away c1
/// is deleted, so nothing she reads rests on d1 or its members any more,
/// but she is still d1's member and reads all three. The reconnect asks the
/// server about them: it ships each as it is now, and evicts none.
#[test]
fn support_still_readable_after_a_reconnect_stays_current() {
    let (d1, m1_ada, m1_bo) = (id(1, 1), id(2, 1), id(2, 2));
    let c1 = id(3, 1);
    let server = open(docs_schema(), 0, AuthorSubject::SYSTEM);
    seed(
        &server,
        "docs",
        d1,
        cells(&[("owner", subject(BO)), ("visibility", "private".into())]),
    );
    for (member, who) in [(m1_ada, ADA), (m1_bo, BO)] {
        seed(
            &server,
            "members",
            member,
            cells(&[("doc_id", uuid(d1)), ("user_id", subject(who))]),
        );
    }
    seed(
        &server,
        "comments",
        c1,
        cells(&[("doc", uuid(d1)), ("body", "c".into())]),
    );
    let tables = ["comments", "docs", "members"];

    let ada = open(docs_schema(), 1, user(ADA));
    let (link, _log) = connect(&server, &ada, user(ADA));
    let comments = ada.prepare_query(&Query::from("comments")).unwrap();
    let _live = block_on(ada.subscribe(&comments, ReadOpts::default())).unwrap();
    pump(&server, &ada);
    assert_eq!(
        ids(&held(&ada, &tables)),
        expect(&[
            ("comments", &[c1]),
            ("docs", &[d1]),
            ("members", &[m1_ada, m1_bo]),
        ])
    );

    assert!(ada.detach_connection(&link));
    block_on(server.delete("comments", c1, Default::default())).unwrap();
    update(&server, "docs", d1, cells(&[("visibility", "team".into())]));
    let (_link, log) = connect(&server, &ada, user(ADA));
    pump(&server, &ada);
    let sent = Sent::take(&log);
    let now = held(&ada, &tables);
    assert_eq!(
        ids(&now),
        expect(&[
            ("comments", &[]),
            ("docs", &[d1]),
            ("members", &[m1_ada, m1_bo]),
        ]),
        "{sent:?}"
    );
    assert!(
        sent.lost.iter().all(|(table, _)| table == "comments"),
        "no support is evicted: {sent:?}"
    );
    // d1 changed while she was away: what she holds is its current version.
    assert_current(&server, &now);
}

/// #5097 across a restart. Ada's replica is durable: her comments read
/// holds c1, with d1 and both of d1's memberships as its support. She
/// closes the app; while it is closed her membership of d1 is deleted, so
/// none of the four is hers to read. When she reopens it, the read resumes
/// from its stored point with the rows it held stored beside it, so the
/// support it no longer holds is re-checked and evicted, as after a
/// reconnect within one process.
///
/// ```text
/// ada ─subscribe comments─► holds c1, d1, m1_ada, m1_bo ─close─► (stored)
/// server: delete m1_ada
/// ada ─reopen, resume─► Holds {c2..} ─re-check d1, m1_*─► AccessLost × 3
/// ```
#[test]
fn support_lost_while_the_replica_was_closed_is_evicted_after_it_reopens() {
    let (d1, d2) = (id(1, 1), id(1, 2));
    let (m1_ada, m1_bo, m2_ada) = (id(2, 1), id(2, 2), id(2, 3));
    let (c1, c2) = (id(3, 1), id(3, 2));
    let server = open(docs_schema(), 0, AuthorSubject::SYSTEM);
    for doc in [d1, d2] {
        seed(
            &server,
            "docs",
            doc,
            cells(&[("owner", subject(BO)), ("visibility", "private".into())]),
        );
    }
    for (member, doc, who) in [(m1_ada, d1, ADA), (m1_bo, d1, BO), (m2_ada, d2, ADA)] {
        seed(
            &server,
            "members",
            member,
            cells(&[("doc_id", uuid(doc)), ("user_id", subject(who))]),
        );
    }
    for (comment, doc) in [(c1, d1), (c2, d2)] {
        seed(
            &server,
            "comments",
            comment,
            cells(&[("doc", uuid(doc)), ("body", "c".into())]),
        );
    }
    let tables = ["comments", "docs", "members"];
    let dir = std::env::temp_dir().join(format!("jazz-support-reopen-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("store.sqlite");
    let open_ada = || {
        block_on(Db::open_at(
            DbConfig::new(
                docs_schema(),
                DbIdentity {
                    node: NodeUuid::from_bytes([1; 16]),
                    author: user(ADA),
                },
            ),
            &path,
            Default::default(),
        ))
        .unwrap()
    };
    let comments = Query::from("comments");
    {
        let ada = open_ada();
        let (link, _log) = connect(&server, &ada, user(ADA));
        let prepared = ada.prepare_query(&comments).unwrap();
        let _live = block_on(ada.subscribe(&prepared, ReadOpts::default())).unwrap();
        pump(&server, &ada);
        assert_eq!(
            ids(&held(&ada, &tables)),
            expect(&[
                ("comments", &[c1, c2]),
                ("docs", &[d1, d2]),
                ("members", &[m1_ada, m1_bo, m2_ada]),
            ])
        );
        assert!(ada.detach_connection(&link));
    }

    block_on(server.delete("members", m1_ada, Default::default())).unwrap();

    let ada = open_ada();
    let (_link, log) = connect(&server, &ada, user(ADA));
    let prepared = ada.prepare_query(&comments).unwrap();
    let _live = block_on(ada.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &ada);
    let sent = Sent::take(&log);
    let now = held(&ada, &tables);
    assert_eq!(
        ids(&now),
        expect(&[("comments", &[c2]), ("docs", &[d2]), ("members", &[m2_ada]),]),
        "after reopening, the support Ada lost while away is gone: {sent:?}"
    );
    assert_eq!(
        distinct(&sent.lost),
        &rows("docs", &[d1]) | &rows("members", &[m1_ada, m1_bo]),
        "the reopened read evicts the support it no longer holds: {sent:?}"
    );
    assert_current(&server, &now);
    drop(ada);
    let _ = std::fs::remove_dir_all(dir);
}

/// Each note's read rests on the note itself through three arms, at every
/// depth. Ada holds exactly the notes she has a role on, gains one when she
/// is given a role, and loses one when her role is taken away offline.
#[test]
fn notes_with_three_self_consulting_arms_ship_exactly_the_readable_notes() {
    let notes: Vec<RowUuid> = (0..6).map(|n| id(4, n)).collect();
    let server = open(notes_schema(), 0, AuthorSubject::SYSTEM);
    // Note n gives Ada role n % 3 (owner, editor, viewer); notes 3.. are
    // nobody's she knows: 4 and 5 give their roles to Bo and Cy.
    for (n, note) in notes.iter().enumerate() {
        let role = |slot: usize| {
            if n < 4 && n % 3 == slot {
                subject(ADA)
            } else {
                subject(if slot == 0 { BO } else { CY })
            }
        };
        seed(
            &server,
            "notes",
            *note,
            cells(&[("owner", role(0)), ("editor", role(1)), ("viewer", role(2))]),
        );
    }

    let ada = open(notes_schema(), 1, user(ADA));
    let (link, log) = connect(&server, &ada, user(ADA));
    let query = ada.prepare_query(&Query::from("notes")).unwrap();
    let _live = block_on(ada.subscribe(&query, ReadOpts::default())).unwrap();
    pump(&server, &ada);
    let now = held(&ada, &["notes"]);
    assert_eq!(
        ids(&now),
        expect(&[("notes", &notes[..4])]),
        "the first read"
    );
    assert_current(&server, &now);
    let sent = Sent::take(&log);
    assert_eq!(
        distinct(&sent.images),
        rows("notes", &notes[..4]),
        "{sent:?}"
    );

    // Live: Ada becomes note 4's viewer.
    update(
        &server,
        "notes",
        notes[4],
        cells(&[("viewer", subject(ADA))]),
    );
    pump(&server, &ada);
    let now = held(&ada, &["notes"]);
    assert_eq!(
        ids(&now),
        expect(&[("notes", &notes[..5])]),
        "after a grant"
    );
    assert_current(&server, &now);
    Sent::take(&log);

    // Offline: note 0 no longer names her.
    assert!(ada.detach_connection(&link));
    update(&server, "notes", notes[0], cells(&[("owner", subject(BO))]));
    let (_link, log) = connect(&server, &ada, user(ADA));
    pump(&server, &ada);
    Sent::take(&log);
    let now = held(&ada, &["notes"]);
    assert_eq!(
        ids(&now),
        expect(&[("notes", &notes[1..5])]),
        "after a revoke while offline"
    );
    assert_current(&server, &now);
}

/// The docs fixture of [`comments_ship_exactly_the_docs_and_members_their_read_rests_on`],
/// before anything changes.
fn seed_docs(server: &Db) {
    for (doc, owner, visibility) in [
        (id(1, 1), BO, "private"),
        (id(1, 2), ADA, "private"),
        (id(1, 3), BO, "world"),
        (id(1, 4), BO, "private"),
    ] {
        seed(
            server,
            "docs",
            doc,
            cells(&[("owner", subject(owner)), ("visibility", visibility.into())]),
        );
    }
    for (member, doc, who) in [
        (id(2, 1), 1, ADA),
        (id(2, 2), 1, BO),
        (id(2, 3), 2, BO),
        (id(2, 4), 3, CY),
        (id(2, 5), 4, CY),
    ] {
        seed(
            server,
            "members",
            member,
            cells(&[("doc_id", uuid(id(1, doc))), ("user_id", subject(who))]),
        );
    }
    for (comment, doc) in [(id(3, 1), 1), (id(3, 2), 2), (id(3, 4), 4)] {
        seed(
            server,
            "comments",
            comment,
            cells(&[("doc", uuid(id(1, doc))), ("body", "c".into())]),
        );
    }
}

/// A bounded inherit (two hops: the doc, then what the doc's read rests
/// on) and a one-shot remote read ship what the live read does.
#[test]
fn bounded_inherits_and_one_shot_reads_ship_the_same_support() {
    let first = expect(&[
        ("comments", &[id(3, 1), id(3, 2)]),
        ("docs", &[id(1, 1), id(1, 2)]),
        ("members", &[id(2, 1), id(2, 2)]),
    ]);
    let tables = ["comments", "docs", "members"];
    for max_depth in [None, Some(2)] {
        let server = open(docs_schema_inheriting(max_depth), 0, AuthorSubject::SYSTEM);
        seed_docs(&server);

        let live = open(docs_schema_inheriting(max_depth), 1, user(ADA));
        let (_link, _log) = connect(&server, &live, user(ADA));
        let comments = live.prepare_query(&Query::from("comments")).unwrap();
        let _sub = block_on(live.subscribe(&comments, ReadOpts::default())).unwrap();
        pump(&server, &live);
        let now = held(&live, &tables);
        assert_eq!(ids(&now), first, "live read, max_depth {max_depth:?}");
        assert_current(&server, &now);

        let once = open(docs_schema_inheriting(max_depth), 2, user(ADA));
        let (_link, _log) = connect(&server, &once, user(ADA));
        pump(&server, &once);
        // As `db.all` in TypeScript issues it.
        let bytes = postcard::to_allocvec(&Query::from("comments")).unwrap();
        let rows = match drive(
            &[&once, &server, &once],
            once.all_serialized_query(
                &bytes,
                ReadOpts::for_read_tier(ReadTier::Remote),
                None,
                None,
                None,
            ),
        )
        .unwrap()
        {
            jazz::db::SerializedReadResult::Rows(rows) => rows,
            other => panic!("{other:?}"),
        };
        assert_eq!(rows.len(), 2, "one-shot read, max_depth {max_depth:?}");
        let now = held(&once, &tables);
        assert_eq!(ids(&now), first, "one-shot read, max_depth {max_depth:?}");
        assert_current(&server, &now);
    }
}

/// While Ada is away, a comment is posted on d2, which she reads as its
/// owner, and her membership of d2 is deleted, so Bo's membership of d2 is
/// no longer hers to read; d2 itself doesn't change. On reconnect, d2's
/// support is reached twice: below the new comment, which ships (so its
/// support is followed whole, with no floor), and below the held one,
/// which diffs against the resume floor. Only the second reports what she
/// lost, so neither visit may hide it.
#[test]
fn support_reached_below_new_and_held_rows_reports_access_lost_offline() {
    let (d2, m2_bo, m2_ada) = (id(1, 2), id(2, 3), id(2, 6));
    // Sorts before c2, so the walk reaches d2 below it first.
    let (c_new, c2) = (id(3, 0), id(3, 2));
    let server = open(docs_schema(), 0, AuthorSubject::SYSTEM);
    seed_docs(&server);
    seed(
        &server,
        "members",
        m2_ada,
        cells(&[("doc_id", uuid(d2)), ("user_id", subject(ADA))]),
    );
    let tables = ["comments", "docs", "members"];

    let ada = open(docs_schema(), 1, user(ADA));
    let (link, _log) = connect(&server, &ada, user(ADA));
    let comments = ada.prepare_query(&Query::from("comments")).unwrap();
    let _live = block_on(ada.subscribe(&comments, ReadOpts::default())).unwrap();
    pump(&server, &ada);
    let now = ids(&held(&ada, &tables));
    assert!(
        now["members"].contains(&m2_bo) && now["members"].contains(&m2_ada),
        "Ada reads d2's members as one of them: {now:?}"
    );

    assert!(ada.detach_connection(&link));
    insert(
        &server,
        "comments",
        c_new,
        cells(&[("doc", uuid(d2)), ("body", "c".into())]),
    );
    block_on(server.delete("members", m2_ada, Default::default())).unwrap();
    let (_link, log) = connect(&server, &ada, user(ADA));
    pump(&server, &ada);
    let sent = Sent::take(&log);
    let now = held(&ada, &tables);
    assert!(
        ids(&now)["comments"].contains(&c_new) && ids(&now)["comments"].contains(&c2),
        "{now:?}"
    );
    assert!(
        sent.lost.contains(&("members".to_owned(), m2_bo)),
        "the server reports Bo's membership of d2 lost: {sent:?}"
    );
    assert!(
        !ids(&now)["members"].contains(&m2_bo),
        "and the replica drops it: {now:?}"
    );
}
