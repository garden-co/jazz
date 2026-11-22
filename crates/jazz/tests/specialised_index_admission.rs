//! What a reader gets from a search depends only on the rows it may read
//! (SPEC 20.7, `INV-IX-3`): its rows, their order, and every refusal, for
//! each kind, at and either side of each cap, one-shot and live.

use std::collections::BTreeMap;
use std::path::Path;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, SubscriptionEvent,
    SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{IndexOp, MatchMode, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder, VectorMetric,
};

use crate::common::{compile_schema, exists, outer_eq, session_eq};

const ME: [&str; 2] = ["identity", "subject"];
/// The postings and candidate caps these tests run under.
const CAP: u32 = 3;
/// The prefix-expansion cap (`PREFIX_TERMS_MAX`).
const PREFIX_CAP: u32 = 1000;

fn reader() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xb0; 16])
}

fn owner() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn subject(author: AuthorSubject) -> Value {
    Value::String(author.principal_parts().1)
}

/// `docs(owner, project, body, location, embedding)` with an index of each
/// kind, readable by its owner or a user a `doc_shares` row names.
fn schema() -> JazzSchema {
    let writes = |read: PolicyExpr| {
        TablePolicies::new()
            .with_select(read)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True)
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("owner", ColumnType::Text)
                    .column("project", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .column("location", ColumnType::Point)
                    .column("embedding", ColumnType::Vector { dimensions: 2 })
                    .index_only(["owner", "project"])
                    .policies(writes(PolicyExpr::or(vec![
                        session_eq("owner", &ME),
                        exists(
                            "doc_shares",
                            vec![outer_eq("doc", "id"), session_eq("user_id", &ME)],
                        ),
                    ])))
                    .text_index("search", [("body", 1)], "jazz-text/1:en", true)
                    .geo_index("near", "location")
                    .vector_index("similar", "embedding", VectorMetric::L2),
            )
            .table(
                TableSchemaBuilder::new("doc_shares")
                    .fk_column("doc", "docs")
                    .column("user_id", ColumnType::Text)
                    .index_only(["doc"])
                    .policies(writes(session_eq("user_id", &ME))),
            )
            .build(),
    )
}

fn config() -> DbConfig {
    jazz::use_test_kinds(false);
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn open() -> Db {
    block_on(Db::open(config())).unwrap()
}

/// Lowers the postings and candidate caps to [`CAP`] while it lives.
struct Caps;

impl Caps {
    fn lowered() -> Self {
        jazz::set_postings_cap_for_test(Some(CAP as usize));
        jazz::set_vector_cap_for_test(Some(CAP as usize));
        Self
    }
}

impl Drop for Caps {
    fn drop(&mut self) {
        jazz::set_postings_cap_for_test(None);
        jazz::set_vector_cap_for_test(None);
    }
}

fn row(n: u32) -> RowUuid {
    let mut id = [0x10; 16];
    id[..4].copy_from_slice(&n.to_be_bytes());
    RowUuid::from_bytes(id)
}

fn options(id: RowUuid) -> InsertOptions {
    InsertOptions {
        row_id: Some(id),
        ..Default::default()
    }
}

fn doc(db: &Db, n: u32, who: AuthorSubject, project: &str, body: &str) {
    block_on(
        db.insert(
            "docs",
            BTreeMap::from([
                ("owner".to_owned(), subject(who)),
                ("project".to_owned(), Value::String(project.into())),
                ("body".to_owned(), Value::String(body.into())),
                (
                    "location".to_owned(),
                    Value::Bytes(
                        jazz::records::encode_point_f64le(13.4, 52.5)
                            .unwrap()
                            .to_vec(),
                    ),
                ),
                (
                    "embedding".to_owned(),
                    Value::Bytes(
                        [n as f32 * 0.01, 0.0f32]
                            .iter()
                            .flat_map(|x| x.to_le_bytes())
                            .collect(),
                    ),
                ),
            ]),
            options(row(n)),
        ),
    )
    .unwrap();
}

fn share(db: &Db, n: u32, doc: u32, with: AuthorSubject) {
    block_on(db.insert(
        "doc_shares",
        BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(row(doc).0)),
            ("user_id".to_owned(), subject(with)),
        ]),
        options(row(100_000 + n)),
    ))
    .unwrap();
}

/// `count` distinct words that start with `qq`.
fn words(count: u32) -> String {
    (0..count)
        .map(|n| format!("qq{n}x"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn text(words: &str) -> Query {
    Query::from("docs").search(
        "search",
        IndexOp::Match {
            text: words.into(),
            mode: MatchMode::Any,
        },
    )
}

fn nearest() -> Query {
    Query::from("docs").search(
        "similar",
        IndexOp::Nearest {
            vector: vec![0.0, 0.0],
            k: 2,
        },
    )
}

fn within() -> Query {
    Query::from("docs").search(
        "near",
        IndexOp::WithinRadius {
            lat: 52.5,
            lng: 13.4,
            meters: 1_000.0,
        },
    )
}

/// What the reader gets: rows in order, or the refusal in full.
fn outcome(db: &Db, query: &Query) -> Result<Vec<RowUuid>, (Option<&'static str>, String)> {
    let prepared = db.prepare_query(query).unwrap();
    block_on(db.all_for_identity(&prepared, ReadOpts::default(), reader()))
        .map(|rows| rows.iter().map(|row| row.row_uuid()).collect())
        .map_err(|error| (error.reason.map(|r| r.as_str()), error.message))
}

/// `queries`' outcomes hold still while rows the reader may not read come
/// and go.
fn unchanged_by_unreadable_rows(db: &Db, queries: &[Query], unreadable: &[&str]) {
    let before: Vec<_> = queries.iter().map(|q| outcome(db, q)).collect();
    for (n, body) in unreadable.iter().enumerate() {
        doc(db, 50_000 + n as u32, owner(), "home", body);
    }
    let with: Vec<_> = queries.iter().map(|q| outcome(db, q)).collect();
    assert_eq!(with, before, "after unreadable rows were added");
    for n in 0..unreadable.len() {
        block_on(db.delete("docs", row(50_000 + n as u32), Default::default())).unwrap();
    }
    let after: Vec<_> = queries.iter().map(|q| outcome(db, q)).collect();
    assert_eq!(after, before, "after they were removed");
}

/// Every kind's result and refusal is the same with or without unreadable
/// rows, with the reader one below, at and one above the postings cap
/// (text) and the candidate cap (vector).
#[test]
fn every_kinds_outcome_depends_only_on_readable_rows() {
    let _caps = Caps::lowered();
    for readable in [CAP - 1, CAP, CAP + 1] {
        let db = open();
        for n in 0..readable {
            doc(&db, n, reader(), "home", "garden");
        }
        let searches = [text("garden"), nearest(), within()];
        for (query, capped) in searches.iter().zip([true, true, false]) {
            let refused = outcome(&db, query).is_err();
            assert_eq!(refused, capped && readable > CAP, "{readable}: {query:?}");
        }
        unchanged_by_unreadable_rows(&db, &searches, &["garden"; 5]);
    }
}

/// The prefix cap counts only terms on rows the reader may read.
#[test]
fn the_prefix_caps_outcome_depends_only_on_readable_rows() {
    for readable in [PREFIX_CAP - 1, PREFIX_CAP, PREFIX_CAP + 1] {
        let db = open();
        doc(&db, 1, reader(), "home", &words(readable));
        assert_eq!(
            outcome(&db, &text("qq")).is_err(),
            readable > PREFIX_CAP,
            "{readable}"
        );
        let other = words(PREFIX_CAP + 200);
        unchanged_by_unreadable_rows(&db, &[text("qq")], &[&other, &other]);
    }
}

/// A row the policy lets the reader read counts, and stops counting when
/// it no longer does, at and either side of each cap.
#[test]
fn a_policy_change_moves_the_caps_only_by_readable_rows() {
    let _caps = Caps::lowered();
    for granted in [CAP - 1, CAP, CAP + 1] {
        let db = open();
        for n in 0..CAP + 3 {
            doc(&db, n, owner(), "home", "garden");
        }
        for query in [text("garden"), nearest()] {
            assert_eq!(outcome(&db, &query), Ok(vec![]), "none readable");
        }
        for n in 0..granted {
            share(&db, n, n, reader());
        }
        for query in [text("garden"), nearest()] {
            assert_eq!(outcome(&db, &query).is_err(), granted > CAP, "{granted}");
        }
        unchanged_by_unreadable_rows(&db, &[text("garden"), nearest()], &["garden"; 4]);
        for n in 0..granted {
            block_on(db.delete("doc_shares", row(100_000 + n), Default::default())).unwrap();
        }
        for query in [text("garden"), nearest()] {
            assert_eq!(outcome(&db, &query), Ok(vec![]), "none readable again");
        }
    }
}

/// The caps count what the rows hold now, not what they held when last
/// indexed.
#[test]
fn the_caps_count_rows_as_they_are_now() {
    let _caps = Caps::lowered();
    let dir = tempfile::tempdir().unwrap();
    let path: &Path = &dir.path().join("jazz.sqlite");
    let db = block_on(Db::open_at(config(), path, FileOptions::default())).unwrap();
    for n in 0..=CAP {
        doc(&db, n, reader(), "home", "garden");
    }
    // A write the engine has only queued yet.
    let body = crate::common::stored(&db, "docs", "body");
    jazz::storage::Storage::open(path)
        .unwrap()
        .execute_batch(&format!("UPDATE docs SET \"{body}\" = 'shed';"))
        .unwrap();
    assert_eq!(outcome(&db, &text("garden")), Ok(vec![]));
}

fn subscribe(db: &Db, query: &Query) -> SubscriptionStream {
    let prepared = db.prepare_query(query).unwrap();
    let mut stream =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), reader())).unwrap();
    block_on(db.tick()).unwrap();
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Rejected { reason, .. } = event {
            panic!("{reason}");
        }
    }
    stream
}

/// What one tick told a live search: the rows entering and leaving it, and
/// whether it was refused.
fn told(db: &Db, stream: &mut SubscriptionStream) -> (Vec<RowUuid>, Vec<RowUuid>, bool) {
    block_on(db.tick()).unwrap();
    let (mut added, mut removed, mut refused) = (Vec::new(), Vec::new(), false);
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Delta {
                added: a,
                removed: r,
                ..
            } => {
                added.extend(a.iter().map(|row| row.row_uuid()));
                removed.extend(r.iter().map(|row| row.row_uuid));
            }
            SubscriptionEvent::Rejected { .. } => refused = true,
            SubscriptionEvent::Closed => {}
        }
    }
    (added, removed, refused)
}

/// A live search is told nothing by writes to rows the reader may not
/// read, and crossing a cap by a policy change is told at that change.
#[test]
fn a_live_searchs_outcome_depends_only_on_readable_rows() {
    let _caps = Caps::lowered();
    for (query, unreadable_first) in [
        (text("garden"), false),
        (text("garden"), true),
        (nearest(), false),
        (nearest(), true),
    ] {
        let db = open();
        for n in 0..CAP + 1 {
            doc(&db, n, owner(), "home", "garden");
        }
        for n in 0..CAP {
            share(&db, n, n, reader());
        }
        let mut stream = subscribe(&db, &query);
        if unreadable_first {
            for n in 0..4 {
                doc(&db, 60_000 + n, owner(), "home", "garden");
                assert_eq!(told(&db, &mut stream), (vec![], vec![], false), "{query:?}");
            }
        }
        share(&db, CAP, CAP, reader());
        assert!(told(&db, &mut stream).2, "{query:?}: over the cap now");
    }
}

/// A live search notices a readable row taking it over a cap at that write,
/// whether or not the row matches the query's other filters, so no later
/// write decides when it is refused.
#[test]
fn a_live_search_crosses_a_cap_at_the_readable_write_that_crosses_it() {
    let _caps = Caps::lowered();
    let at_home =
        |query: Query| query.filter(eq(col("project"), lit(Value::String("home".into()))));
    // Text counts the readable rows holding the words, whatever their
    // project; vector the readable rows the query's filters match.
    for (query, project) in [
        (at_home(text("garden")), "work"),
        (at_home(nearest()), "home"),
    ] {
        let db = open();
        for n in 0..CAP {
            doc(&db, n, reader(), "home", "garden");
        }
        let mut stream = subscribe(&db, &query);
        // Farther than every row in a nearest search's window.
        doc(&db, 900, reader(), project, "garden");
        assert!(told(&db, &mut stream).2, "{query:?}");
    }
}
