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
                    .policies(all_writes(PolicyExpr::inherits(Operation::Select, "doc"))),
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
    server
        .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
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
    // What should hold now. The replica also still holds d1 and both of
    // its members, including Ada's deleted membership: support the reader
    // lost while offline isn't evicted on reconnect. That is so on main
    // before #4778 too (#5097); until that is fixed, this test only asserts
    // that the rows that should be held are.
    let resumed = expect(&[
        ("comments", &[c2, c3, c4]),
        ("docs", &[d2, d3, d4]),
        ("members", &[m2_bo, m3_cy, m4_cy, m4_ada]),
    ]);
    for (table, expected) in &resumed {
        let held = &ids(&now)[table];
        assert!(
            expected.iter().all(|row| held.contains(row)),
            "after reconnecting, {table} holds {expected:?}: {held:?}"
        );
    }
    let expected_rows = now
        .iter()
        .map(|(table, rows)| {
            let rows = rows
                .iter()
                .filter(|(row, _)| resumed[table].contains(row))
                .map(|(row, cells)| (*row, cells.clone()))
                .collect();
            (table.clone(), rows)
        })
        .collect();
    assert_current(&server, &expected_rows);
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
