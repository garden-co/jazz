//! Specialised indexes (SPEC 20): the framework every kind shares. A
//! search returns the rows its index and every other clause match, best
//! first; the index follows every write that reaches the current table, on
//! every node, in the same transaction; declarations build, rebuild and
//! drop with the schema; and the shapes the framework refuses fail with
//! typed errors.
//!
//! The kinds here are the tiny test kinds of `jazz_db::ix::test_kind`
//! (`jazz::use_test_kinds`): `words` implements text indexes (lowercased
//! whitespace words, score = weighted occurrences) on the SQL path and
//! `near` implements vector indexes (squared L2 over `f32le` bytes) on the
//! Rust-scored path. The real kinds' own tests live with them.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, Reason, SubscriptionEvent,
    SyncMessage, Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{IndexOp, MatchMode, OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder, Value as PublicValue, VectorMetric,
};

use crate::common::{allow_all_policies, compile_schema};

/// How the `notes` table declares its indexes.
#[derive(Clone, Copy)]
enum Indexes {
    None,
    /// `search` over title (weight 2) and body (1), `similar` over
    /// `embedding`.
    Both,
    /// `search` over the title only.
    TitleOnly,
    /// [`Indexes::Both`] with `similar` server-only.
    VectorServerOnly,
}

/// `notes(title, body?, project, embedding)`, `project` B-tree indexed.
/// `inserts` restricts which rows the authority admits (`project` =
/// "open"), for rejected writes.
fn schema_with(indexes: Indexes, restrict_inserts: bool) -> JazzSchema {
    let mut policies = allow_all_policies();
    if restrict_inserts {
        policies = policies.with_insert(PolicyExpr::eq_literal(
            "project",
            PublicValue::Text("open".into()),
        ));
    }
    let mut table = TableSchemaBuilder::new("notes")
        .column("title", ColumnType::Text)
        .nullable_column("body", ColumnType::Text)
        .column("project", ColumnType::Text)
        .column("embedding", ColumnType::Vector { dimensions: 2 })
        .index_only(["project"])
        .policies(policies);
    table = match indexes {
        Indexes::None => table,
        Indexes::Both => table
            .text_index(
                "search",
                [("title", 2), ("body", 1)],
                "jazz-text/1:en",
                false,
            )
            .vector_index("similar", "embedding", VectorMetric::L2),
        Indexes::TitleOnly => table.text_index("search", [("title", 1)], "jazz-text/1:en", false),
        Indexes::VectorServerOnly => table
            .text_index(
                "search",
                [("title", 2), ("body", 1)],
                "jazz-text/1:en",
                false,
            )
            .vector_index("similar", "embedding", VectorMetric::L2)
            .server_only(),
    };
    compile_schema(&SchemaBuilder::new().table(table).build())
}

fn schema() -> JazzSchema {
    schema_with(Indexes::Both, false)
}

fn open(node: u8, schema: JazzSchema) -> Db {
    open_as(node, schema, AuthorSubject::SYSTEM)
}

fn open_as(node: u8, schema: JazzSchema, author: AuthorSubject) -> Db {
    jazz::use_test_kinds(true);
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

fn authority(schema: JazzSchema) -> Db {
    let db = open(0, schema);
    db.set_authority();
    db
}

fn client(node: u8, schema: JazzSchema) -> Db {
    let db = open_as(node, schema, alice());
    db.become_replica();
    db
}

struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn link(client: &Db, server: &Db) -> jazz::db::Upstream {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let link = block_on(client.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Pipe {
                out: down,
                inbound: up,
            }),
            alice(),
        )
        .unwrap();
    link
}

/// The author every client writes as: not the system, so the authority's
/// policies apply to her writes.
fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..8 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn f32le(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

fn note(
    title: &str,
    body: Option<&str>,
    project: &str,
    embedding: &[f32],
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        (
            "body".to_owned(),
            Value::Nullable(body.map(|b| Box::new(Value::String(b.to_owned())))),
        ),
        ("project".to_owned(), Value::String(project.to_owned())),
        ("embedding".to_owned(), Value::Bytes(f32le(embedding))),
    ])
}

fn insert(db: &Db, id: u8, cells: BTreeMap<String, Value>) {
    block_on(db.insert(
        "notes",
        cells,
        InsertOptions {
            row_id: Some(row(id)),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn update(db: &Db, id: u8, column: &str, value: Value) {
    block_on(db.update(
        "notes",
        row(id),
        BTreeMap::from([(column.to_owned(), value)]),
        Default::default(),
    ))
    .unwrap();
}

fn text(words: &str) -> IndexOp {
    IndexOp::Match {
        text: words.into(),
        mode: MatchMode::All,
    }
}

fn nearest(vector: &[f32], k: u32) -> IndexOp {
    IndexOp::Nearest {
        vector: vector.to_vec(),
        k,
    }
}

/// The ids (by their first byte) `query` reads, in order.
fn ids(db: &Db, query: &Query) -> Vec<u8> {
    try_ids(db, query).unwrap()
}

fn try_ids(db: &Db, query: &Query) -> Result<Vec<u8>, jazz::db::Error> {
    let prepared = db.prepare_query(query)?;
    Ok(block_on(db.all(&prepared, ReadOpts::default()))?
        .iter()
        .map(|row| row.row_uuid().0.as_bytes()[0])
        .collect())
}

fn search(words: &str) -> Query {
    Query::from("notes").search("search", text(words))
}

/// Three notes on the authority; alice searches them.
fn seeded() -> Db {
    let db = authority(schema());
    insert(
        &db,
        1,
        note(
            "Garden plans",
            Some("tomatoes and garden beans"),
            "home",
            &[0.0, 0.0],
        ),
    );
    insert(&db, 2, note("Tomato soup", None, "kitchen", &[1.0, 0.0]));
    insert(
        &db,
        3,
        note("Garden tools", Some("a rake"), "home", &[5.0, 5.0]),
    );
    db
}

/// A search returns the rows matching every query word in some field,
/// best first by score (title words weigh twice), ties by id; it composes
/// with filters, explicit orders, limits and offsets like any filter.
#[test]
fn a_search_returns_matching_rows_best_first_and_composes_with_the_query() {
    let db = seeded();
    // Row 1 has "garden" in its title (2) and its body (1): 3. Row 3 only
    // in its title: 2.
    assert_eq!(ids(&db, &search("garden")), [1, 3]);
    assert_eq!(ids(&db, &search("GARDEN tools")), [3]);
    assert_eq!(ids(&db, &search("garden soup")), Vec::<u8>::new());
    assert_eq!(
        ids(
            &db,
            &search("garden").filter(eq(col("project"), lit(Value::String("home".into()))))
        ),
        [1, 3]
    );
    assert_eq!(ids(&db, &search("garden").limit(1)), [1]);
    assert_eq!(ids(&db, &search("garden").offset(1)), [3]);
    assert_eq!(
        ids(
            &db,
            &search("garden").order_by("$score", OrderDirection::Desc)
        ),
        [3, 1]
    );
    assert_eq!(
        ids(
            &db,
            &search("garden").order_by("title", OrderDirection::Asc)
        ),
        [1, 3]
    );
}

/// Every local write reaches the index in its own transaction: inserts,
/// updates of an indexed column, deletes and restores. An update of a
/// column no index reads leaves the index alone (it is not queued).
#[test]
fn local_writes_reach_the_index_in_their_transaction() {
    let db = seeded();
    update(&db, 2, "title", Value::String("Garden soup".into()));
    // Row 1 scores 3, rows 2 and 3 score 2 each.
    assert_eq!(ids(&db, &search("garden")), [1, 2, 3]);
    update(&db, 1, "body", Value::Nullable(None));
    // Row 1 now scores 2 like rows 2 and 3: ties order by id.
    assert_eq!(ids(&db, &search("garden")), [1, 2, 3]);
    update(&db, 3, "project", Value::String("work".into()));
    assert_eq!(ids(&db, &search("garden")), [1, 2, 3]);
    block_on(db.delete("notes", row(2), Default::default())).unwrap();
    assert_eq!(ids(&db, &search("garden")), [1, 3]);
    block_on(db.restore("notes", row(2), None, Default::default())).unwrap();
    assert_eq!(ids(&db, &search("garden")), [1, 2, 3]);
    // Several writes in one transaction are indexed when it commits.
    block_on(db.transaction(async |tx| {
        tx.update(
            "notes",
            row(1),
            BTreeMap::from([("title".to_owned(), Value::String("Shed".into()))]),
            Default::default(),
        )
        .await?;
        tx.update(
            "notes",
            row(3),
            BTreeMap::from([("title".to_owned(), Value::String("Shed tools".into()))]),
            Default::default(),
        )
        .await?;
        Ok(())
    }))
    .unwrap();
    assert_eq!(ids(&db, &search("shed")), [1, 3]);
    assert_eq!(ids(&db, &search("garden")), [2]);
}

/// Rows a client receives from the authority are indexed on arrival, and
/// its own pending writes as it makes them; a write the authority rejects
/// leaves the client's index when the pending version is removed.
///
/// ```
/// server ──row 1, row 2──► bob (indexed on arrival)
/// bob ──insert row 4 (project "closed")──► server ──✗ insert policy
///          bob searches row 4 while pending, not after its removal
/// ```
#[test]
fn synced_versions_and_removed_pending_writes_reach_a_clients_index() {
    let server = authority(schema_with(Indexes::Both, true));
    insert(&server, 1, note("Garden plans", None, "open", &[0.0, 0.0]));
    insert(&server, 2, note("Tomato soup", None, "open", &[1.0, 0.0]));
    let bob = client(1, schema());
    let _link = link(&bob, &server);
    let all = bob.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(bob.subscribe(&all, ReadOpts::default())).unwrap();
    pump(&server, &[&bob]);
    assert_eq!(ids(&bob, &search("garden")), [1]);

    // The server's update arrives as a synced version.
    update(&server, 2, "title", Value::String("Garden soup".into()));
    pump(&server, &[&bob]);
    assert_eq!(ids(&bob, &search("garden")), [1, 2]);

    insert(&bob, 4, note("Garden secrets", None, "closed", &[2.0, 2.0]));
    assert_eq!(
        ids(&bob, &search("secrets")),
        [4],
        "pending, searchable at once"
    );
    pump(&server, &[&bob]);
    assert_eq!(
        ids(&bob, &Query::from("notes")),
        [1, 2],
        "the rejected row is gone"
    );
    assert_eq!(
        ids(&bob, &search("secrets")),
        Vec::<u8>::new(),
        "the rejected write left the index with its row"
    );
    assert_eq!(ids(&server, &search("garden")), [1, 2]);
}

/// A schema that adds an index builds it over the rows already stored, in
/// bounded batches; one that changes it rebuilds it; one that drops it
/// drops its shadow tables. Until a build finishes, the authority refuses
/// searches with a retryable `index_building` error.
#[test]
fn declarations_build_rebuild_and_drop_with_the_schema() {
    let db = authority(schema_with(Indexes::None, false));
    for n in 0..1200u32 {
        let id = RowUuid::from_bytes({
            let mut b = [0u8; 16];
            b[..4].copy_from_slice(&n.to_be_bytes());
            b
        });
        let title = if n % 100 == 7 { "garden" } else { "other" };
        block_on(db.insert(
            "notes",
            note(title, None, "home", &[0.0, 0.0]),
            InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    block_on(db.activate_schema(schema_with(Indexes::TitleOnly, false))).unwrap();
    let error = try_ids(&db, &search("garden")).expect_err("still building");
    assert_eq!(error.code, ErrorCode::Backpressure);
    assert_eq!(error.reason, Some(Reason::IndexBuilding));
    // Each tick builds one batch; 1200 rows take three.
    for _ in 0..3 {
        block_on(db.tick()).unwrap();
    }
    let count = |db: &Db, words: &str| try_ids(db, &search(words)).map(|ids| ids.len());
    assert_eq!(count(&db, "garden").unwrap(), 12);

    // Weighting the field anew is a new declaration: rebuilt.
    block_on(db.activate_schema(schema_with(Indexes::Both, false))).unwrap();
    for _ in 0..3 {
        block_on(db.tick()).unwrap();
    }
    assert_eq!(count(&db, "garden").unwrap(), 12);

    block_on(db.activate_schema(schema_with(Indexes::None, false))).unwrap();
    block_on(db.tick()).unwrap();
    let error = try_ids(&db, &search("garden")).expect_err("no index any more");
    assert!(
        error.message.contains("no specialised index `search`"),
        "{error}"
    );
}

/// A search is a top-level filter, at most one per query; the index must
/// exist and be of the op's kind; `$score` orders searches only.
#[test]
fn misplaced_or_mismatched_searches_are_refused_by_name() {
    let db = seeded();
    let refused = |query: Query| try_ids(&db, &query).expect_err("refused").to_string();
    assert!(
        refused(Query::from("notes").search("nope", text("x")))
            .contains("table notes has no specialised index `nope`")
    );
    assert!(
        refused(Query::from("notes").search("similar", text("x")))
            .contains("index notes.similar is a vector index; `match` searches a text index")
    );
    assert!(
        refused(search("a").search("search", text("b")))
            .contains("a query can search only one index")
    );
    assert!(
        refused(Query::from("notes").order_by("$score", OrderDirection::Asc))
            .contains("ordering by $score needs an index search")
    );
    let nested = Query::from("notes").filter(jazz::query::Predicate::Not(Box::new(
        jazz::query::Predicate::Search(jazz::query::IndexSearch {
            index: "search".into(),
            op: text("garden"),
        }),
    )));
    assert!(refused(nested).contains("valid only as a top-level filter"));
    assert!(
        refused(Query::from("notes").search("similar", nearest(&[0.0, 0.0], 0)))
            .contains("k 0 is outside 1..1000")
    );
    assert!(
        refused(Query::from("notes").search("similar", nearest(&[f32::NAN, 0.0], 3)))
            .contains("non-finite value at 0")
    );
}

/// A vector search scores in Rust the rows the query's other clauses and
/// policy match (filter-first) and keeps the `k` best by (score, id). On
/// the authority those clauses must narrow the table through a B-tree
/// index; an unfiltered search is refused rather than scanning the table.
#[test]
fn vector_searches_are_filter_first_and_bounded_on_the_authority() {
    let db = seeded();
    insert(&db, 4, note("Far", None, "home", &[9.0, 9.0]));
    let home =
        || Query::from("notes").filter(eq(col("project"), lit(Value::String("home".into()))));
    assert_eq!(
        ids(&db, &home().search("similar", nearest(&[4.0, 4.0], 2))),
        [3, 1]
    );
    assert_eq!(
        ids(&db, &home().search("similar", nearest(&[4.0, 4.0], 10))),
        [3, 1, 4],
        "squared distances 2, 32 and 50"
    );
    let error = try_ids(
        &db,
        &Query::from("notes").search("similar", nearest(&[0.0, 0.0], 2)),
    )
    .expect_err("unfiltered on the authority");
    assert_eq!(error.reason, Some(Reason::VectorSearchUnbounded));

    // A client scores the rows it holds.
    let alice = open(2, schema());
    insert(&alice, 1, note("a", None, "home", &[0.0, 0.0]));
    insert(&alice, 2, note("b", None, "kitchen", &[1.0, 0.0]));
    insert(&alice, 3, note("c", None, "home", &[1.0, 0.0]));
    assert_eq!(
        ids(
            &alice,
            &Query::from("notes").search("similar", nearest(&[1.0, 0.0], 2))
        ),
        [2, 3],
        "equal scores order by id"
    );
}

/// A server-only index is not maintained on clients: a client's local
/// search through it is refused with `index_server_only`, while its other
/// indexes work.
#[test]
fn a_server_only_index_is_refused_on_a_client() {
    let alice = client(2, schema_with(Indexes::VectorServerOnly, false));
    insert(&alice, 1, note("Garden", None, "home", &[0.0, 0.0]));
    assert_eq!(ids(&alice, &search("garden")), [1]);
    let error = try_ids(
        &alice,
        &Query::from("notes").search("similar", nearest(&[0.0, 0.0], 1)),
    )
    .expect_err("server-only");
    assert_eq!(error.reason, Some(Reason::IndexServerOnly));
    assert!(
        error
            .message
            .contains("index `notes.similar` is server-only"),
        "{error}"
    );
}

/// A subscription with a search updates exactly as the query re-run
/// would: a row that starts matching joins it in score order, one that
/// stops matching leaves it.
#[test]
fn a_live_search_follows_its_rows() {
    let db = seeded();
    let prepared = db.prepare_query(&search("garden")).unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    block_on(db.tick()).unwrap();
    let seen = |stream: &mut jazz::db::SubscriptionStream| {
        let mut added = Vec::new();
        let mut removed = Vec::new();
        while let Some(event) = stream.try_next_event() {
            match event {
                SubscriptionEvent::Delta {
                    added: a,
                    removed: r,
                    ..
                } => {
                    added.extend(a.iter().map(|row| row.row_uuid().0.as_bytes()[0]));
                    removed.extend(r.iter().map(|row| row.row_uuid.0.as_bytes()[0]));
                }
                SubscriptionEvent::Rejected { reason, .. } => panic!("{reason}"),
                SubscriptionEvent::Closed => {}
            }
        }
        (added, removed)
    };
    assert_eq!(seen(&mut stream), (vec![1, 3], vec![]));
    update(&db, 2, "title", Value::String("Garden soup".into()));
    block_on(db.tick()).unwrap();
    assert_eq!(seen(&mut stream), (vec![2], vec![]));
    update(&db, 1, "title", Value::String("Plans".into()));
    update(&db, 1, "body", Value::Nullable(None));
    block_on(db.tick()).unwrap();
    assert_eq!(seen(&mut stream), (vec![], vec![1]));
    assert_eq!(ids(&db, &search("garden")), [2, 3]);
}

/// A transport that says whether its upstream offered index search.
struct Gated {
    inner: Box<dyn Transport>,
    search: bool,
}

impl Transport for Gated {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.inner.send(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }

    fn supports(&self, feature: jazz::wire::WireFeatures) -> bool {
        feature != jazz::wire::FEATURE_INDEX_SEARCH || self.search
    }
}

/// A query with a search goes upstream only to an upstream that offered
/// `FEATURE_INDEX_SEARCH` (an older server could not decode it): at
/// `Remote` tier it is refused, typed; otherwise the replica answers it
/// alone. Queries without a search sync as before.
///
/// ```
/// bob ──Read(search)──✗ (no FEATURE_INDEX_SEARCH) server
///  └── Remote: index_search_unsupported_upstream; LocalFirst: local rows
/// ```
#[test]
fn a_search_goes_upstream_only_to_an_upstream_that_offered_it() {
    let server = authority(schema());
    insert(&server, 1, note("Garden plans", None, "home", &[0.0, 0.0]));
    let bob = client(1, schema());
    let (up, down) = jazz::db::duplex();
    let _link = block_on(bob.connect_upstream(Box::new(Gated {
        inner: up,
        search: false,
    })));
    server.accept_subscriber(down, alice()).unwrap();
    let all = bob.prepare_query(&Query::from("notes")).unwrap();
    let _all = block_on(bob.subscribe(&all, ReadOpts::default())).unwrap();
    pump(&server, &[&bob]);
    assert_eq!(
        ids(&bob, &search("garden")),
        [1],
        "the plain query synced the row"
    );

    let remote = ReadOpts {
        tier: jazz::db::ReadTier::Remote,
        ..ReadOpts::default()
    };
    let prepared = bob.prepare_query(&search("garden")).unwrap();
    let Err(error) = block_on(bob.subscribe(&prepared, remote)) else {
        panic!("a Remote search over this link is refused");
    };
    assert_eq!(error.reason, Some(Reason::IndexSearchUnsupportedUpstream));
    assert!(
        error
            .message
            .contains("the server does not support index search"),
        "{error}"
    );

    let mut local = block_on(bob.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &[&bob]);
    let mut rows = Vec::new();
    while let Some(event) = local.try_next_event() {
        match event {
            SubscriptionEvent::Delta { added, .. } => {
                rows.extend(added.iter().map(|row| row.row_uuid().0.as_bytes()[0]))
            }
            SubscriptionEvent::Rejected { reason, .. } => panic!("{reason}"),
            SubscriptionEvent::Closed => {}
        }
    }
    assert_eq!(rows, [1], "answered by the replica");
}

/// A row whose large value reaches a replica before its chunks is indexed
/// without that value at first and again once the chunks arrive, so the
/// replica's search ends up equal to the authority's (`INV-IX-1`).
///
/// ```
/// server ──row 1 (body chunked)──► bob   search "zebra": not yet
/// server ──chunks──► bob                 search "zebra": [1], as server
/// ```
#[test]
fn a_replica_indexes_a_large_value_once_its_chunks_arrive() {
    let server = authority(schema());
    let mut body = String::from("zebra crossing ");
    while body.len() <= jazz::large_values::INLINE_VALUE_MAX_BYTES * 4 {
        body.push_str("lorem ipsum dolor sit amet ");
    }
    insert(&server, 1, note("Plans", Some(&body), "home", &[0.0, 0.0]));
    assert_eq!(ids(&server, &search("zebra")), [1]);
    let bob = client(1, schema());
    let _link = link(&bob, &server);
    let all = bob.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(bob.subscribe(&all, ReadOpts::default())).unwrap();
    pump(&server, &[&bob]);
    pump(&server, &[&bob]);
    assert_eq!(ids(&bob, &search("zebra")), ids(&server, &search("zebra")));
    assert_eq!(ids(&bob, &search("plans")), [1]);
}

/// A live text search applies a changed row by re-checking it alone and
/// splicing it in by `(score, id)` (SPEC 20.8): unchanged rows keep their
/// scores and places, the changed row moves to where its new score puts
/// it, and the window is the one the query re-run reads (`INV-IX-6`).
/// A live nearest search re-runs instead: its `k` caps the rows it reads.
#[test]
fn a_live_text_search_splices_a_changed_row_by_its_score() {
    let db = seeded();
    insert(
        &db,
        4,
        note("Garden", Some("garden garden"), "home", &[0.0, 0.0]),
    );
    // Scores: row 4 = 2 + 2, row 1 = 2 + 1, row 3 = 2.
    assert_eq!(ids(&db, &search("garden")), [4, 1, 3]);
    let prepared = db.prepare_query(&search("garden").limit(2)).unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    block_on(db.tick()).unwrap();
    while stream.try_next_event().is_some() {}
    let before = db.fanout_stats();
    // Row 3 gains "garden" in its body: 2 + 1 + 1 = 4, tied with row 4,
    // which it follows by id.
    update(
        &db,
        3,
        "body",
        Value::Nullable(Some(Box::new(Value::String("garden garden".into())))),
    );
    block_on(db.tick()).unwrap();
    let after = db.fanout_stats();
    assert!(after.rechecked > before.rechecked, "{before:?} → {after:?}");
    let mut added = Vec::new();
    let mut removed = Vec::new();
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta {
            added: a,
            removed: r,
            ..
        } = event
        {
            added.extend(a.iter().map(|row| row.row_uuid().0.as_bytes()[0]));
            removed.extend(r.iter().map(|row| row.row_uuid.0.as_bytes()[0]));
        }
    }
    assert_eq!((added, removed), (vec![3], vec![1]));
    assert_eq!(ids(&db, &search("garden").limit(2)), [3, 4]);
}

/// A rebase reaches the index: bob's pending edit of one field replays
/// over the authority's concurrent edit of another, and his index follows
/// the rebased row, as the authority's follows the merged one.
///
/// ```
/// server: body := "garden beans"      bob (pending): title := "Shed"
///        ──synced version──► bob rebases: title "Shed", body "garden beans"
/// ```
#[test]
fn a_rebased_pending_write_reaches_the_index() {
    let server = authority(schema());
    insert(&server, 1, note("Plans", None, "open", &[0.0, 0.0]));
    let bob = client(1, schema());
    let _link = link(&bob, &server);
    let all = bob.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(bob.subscribe(&all, ReadOpts::default())).unwrap();
    pump(&server, &[&bob]);
    update(&bob, 1, "title", Value::String("Shed".into()));
    update(
        &server,
        1,
        "body",
        Value::Nullable(Some(Box::new(Value::String("garden beans".into())))),
    );
    // Bob's tick receives the server's version before his upload settles.
    block_on(server.tick()).unwrap();
    block_on(bob.tick()).unwrap();
    assert_eq!(ids(&bob, &search("shed garden")), [1]);
    pump(&server, &[&bob]);
    assert_eq!(ids(&bob, &search("shed garden")), [1]);
    assert_eq!(ids(&server, &search("shed garden")), [1]);
    assert_eq!(ids(&bob, &search("plans")), Vec::<u8>::new());
}

/// An eviction reaches the index: when bob loses read access to a row,
/// the authority evicts it from his replica, and his search stops
/// returning it in the same transaction that drops the row.
#[test]
fn an_evicted_row_leaves_the_replicas_index() {
    let read_open = |restrict: bool| {
        let mut policies = allow_all_policies();
        if restrict {
            policies = policies.with_select(PolicyExpr::eq_literal(
                "project",
                PublicValue::Text("open".into()),
            ));
        }
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("title", ColumnType::Text)
                        .nullable_column("body", ColumnType::Text)
                        .column("project", ColumnType::Text)
                        .column("embedding", ColumnType::Vector { dimensions: 2 })
                        .index_only(["project"])
                        .policies(policies)
                        .text_index(
                            "search",
                            [("title", 2), ("body", 1)],
                            "jazz-text/1:en",
                            false,
                        )
                        .vector_index("similar", "embedding", VectorMetric::L2),
                )
                .build(),
        )
    };
    let server = authority(read_open(true));
    insert(&server, 1, note("Garden plans", None, "open", &[0.0, 0.0]));
    insert(&server, 2, note("Garden tools", None, "open", &[0.0, 0.0]));
    let bob = client(1, read_open(true));
    let _link = link(&bob, &server);
    let all = bob.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(bob.subscribe(&all, ReadOpts::default())).unwrap();
    pump(&server, &[&bob]);
    assert_eq!(ids(&bob, &search("garden")), [1, 2]);
    update(&server, 1, "project", Value::String("secret".into()));
    pump(&server, &[&bob]);
    assert_eq!(ids(&bob, &Query::from("notes")), [2], "evicted");
    assert_eq!(ids(&bob, &search("garden")), [2]);
}

fn search_any(words: &str) -> Query {
    Query::from("notes").search(
        "search",
        IndexOp::Match {
            text: words.into(),
            mode: MatchMode::Any,
        },
    )
}

/// A text search's mode says how many of its terms a row needs: `Any`
/// (the default) at least one, `All` every one. Either way a row ranks by
/// the summed impacts of the terms it holds, so a row holding more of them
/// ranks higher.
#[test]
fn a_text_search_matches_any_or_all_of_its_terms() {
    let db = seeded();
    // Row 1: "garden" 2 + 1 = 3; row 2: "soup" 2; row 3: "garden" 2.
    assert_eq!(ids(&db, &search_any("garden soup")), [1, 2, 3]);
    assert_eq!(ids(&db, &search("garden soup")), Vec::<u8>::new());
    // Row 3 holds both terms: 2 + 2 = 4, above row 1's 3.
    assert_eq!(ids(&db, &search_any("garden tools")), [3, 1]);
    assert_eq!(ids(&db, &search("garden tools")), [3]);
    assert_eq!(
        IndexOp::Match {
            text: String::new(),
            mode: Default::default()
        },
        IndexOp::Match {
            text: String::new(),
            mode: MatchMode::Any
        },
        "Any is the default"
    );
}

/// A live `Any` search re-checks a row that gains a second query term and
/// moves it up by its new score, as the query re-run would order it.
#[test]
fn a_live_any_search_moves_a_row_up_when_it_gains_a_term() {
    let db = seeded();
    let prepared = db.prepare_query(&search_any("garden soup")).unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    block_on(db.tick()).unwrap();
    while stream.try_next_event().is_some() {}
    let before = db.fanout_stats();
    // Row 3 gains "soup" in its body: 2 + 1 = 3, tied with row 1 (by id).
    update(
        &db,
        3,
        "body",
        Value::Nullable(Some(Box::new(Value::String("soup".into())))),
    );
    // And row 2 gains "garden": 2 + 1 = 3 too, ahead of row 3 by id.
    update(
        &db,
        2,
        "body",
        Value::Nullable(Some(Box::new(Value::String("garden".into())))),
    );
    block_on(db.tick()).unwrap();
    assert!(db.fanout_stats().rechecked > before.rechecked);
    let mut updated = Vec::new();
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta { updated: u, .. } = event {
            updated.extend(u.iter().map(|row| row.row_uuid().0.as_bytes()[0]));
        }
    }
    updated.sort();
    assert_eq!(updated, [2, 3]);
    assert_eq!(ids(&db, &search_any("garden soup")), [1, 2, 3]);
    // Row 3 gains "garden" twice more: 2 + 1 + 2 = 5, first.
    update(
        &db,
        3,
        "body",
        Value::Nullable(Some(Box::new(Value::String("soup garden garden".into())))),
    );
    block_on(db.tick()).unwrap();
    assert_eq!(ids(&db, &search_any("garden soup")), [3, 1, 2]);
}
