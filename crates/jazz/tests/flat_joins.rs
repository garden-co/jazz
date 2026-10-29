//! Flat joins (`Query::flat_join`): one output row per matching
//! tuple of the root and each joined source, every source live and read
//! policy filtered, cells qualified as `<source>.<column>`. A row's
//! occurrence is its root id followed by its joined rows, and maintained
//! results add, remove and replace by that occurrence (SPEC 6.4.1). Synced
//! reads ship each occurrence's joined rows.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, ReadTier, SubscriptionEvent,
    SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema, read_and_allow_all_writes, session_eq};

fn schema(posts_read: Option<jazz::tools::PolicyExpr>) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("posts")
                    .column("author_name", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .policies(match posts_read {
                        Some(read) => read_and_allow_all_writes(read),
                        None => allow_all_policies(),
                    }),
            )
            .build(),
    )
}

fn open(schema: JazzSchema, node: u8) -> Db {
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
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

fn user(n: u8) -> RowUuid {
    id(1, n)
}

fn post(n: u8) -> RowUuid {
    id(2, n)
}

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

fn insert(db: &Db, table: &str, row: RowUuid, cells: &[(&str, &str)]) {
    let cells: BTreeMap<String, Value> = cells
        .iter()
        .map(|(name, value)| ((*name).to_owned(), text(value)))
        .collect();
    block_on(db.insert(
        table,
        cells,
        jazz::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn post_by(db: &Db, n: u8, author: &str, title: &str, owner: &str) {
    insert(
        db,
        "posts",
        post(n),
        &[("author_name", author), ("title", title), ("owner", owner)],
    );
}

/// Ada (two posts) and Bob (one); one post by nobody.
fn seed(db: &Db) {
    insert(db, "users", user(1), &[("name", "ada")]);
    insert(db, "users", user(2), &[("name", "bob")]);
    post_by(db, 1, "ada", "a1", "ada");
    post_by(db, 2, "ada", "a2", "bob");
    post_by(db, 3, "bob", "b1", "bob");
    post_by(db, 4, "zed", "z1", "zed");
}

fn by_author() -> Query {
    Query::from("users").flat_join("posts", "users.name", "posts.author_name")
}

fn cell(row: &CurrentRow, name: &str) -> Option<String> {
    match row.cells().find(|(n, _)| *n == name)?.1 {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Nullable(Some(value))) => match &**value {
            Value::String(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// (user name, post title, joined rows) of each row.
fn tuples(rows: &[CurrentRow]) -> Vec<(String, String, Vec<RowUuid>)> {
    rows.iter()
        .map(|row| {
            (
                cell(row, "users.name").unwrap_or_default(),
                cell(row, "posts.title").unwrap_or_default(),
                row.joined_rows().iter().map(|joined| joined.row).collect(),
            )
        })
        .collect()
}

fn read(db: &Db, query: &Query) -> Vec<CurrentRow> {
    block_on(db.all(&db.prepare_query(query).unwrap(), ReadOpts::default())).unwrap()
}

fn error_of(db: &Db, query: &Query) -> String {
    match db.prepare_query(query) {
        Err(error) => error.message,
        Ok(prepared) => match block_on(db.all(&prepared, ReadOpts::default())) {
            Err(error) => error.message,
            Ok(rows) => panic!("expected an error, read {}", rows.len()),
        },
    }
}

fn tuple(user: &str, title: &str, post: RowUuid) -> (String, String, Vec<RowUuid>) {
    (user.to_owned(), title.to_owned(), vec![post])
}

#[test]
fn flat_join_reads_one_row_per_matching_tuple() {
    let db = open(schema(None), 1);
    seed(&db);
    let rows = read(&db, &by_author());
    assert_eq!(
        tuples(&rows),
        [
            tuple("ada", "a1", post(1)),
            tuple("ada", "a2", post(2)),
            tuple("bob", "b1", post(3)),
        ],
        "one row per (user, post) pair, ordered by occurrence"
    );
    // The root is the representative id; every source's columns are there.
    assert_eq!(rows[0].row_uuid(), user(1));
    assert_eq!(cell(&rows[0], "posts.author_name").as_deref(), Some("ada"));
    assert_eq!(cell(&rows[0], "posts.owner").as_deref(), Some("ada"));

    // Filters name qualified columns, or unqualified ones one source has.
    let titled = by_author().filter(eq(col("title"), lit("b1")));
    assert_eq!(tuples(&read(&db, &titled)), [tuple("bob", "b1", post(3))]);
    let named = by_author().filter(eq(col("users.name"), lit("ada")));
    assert_eq!(read(&db, &named).len(), 2);

    let spanning = by_author().filter(eq(col("users.name"), col("posts.owner")));
    let message = error_of(&db, &spanning);
    assert!(
        message.contains("spanning multiple sources") && message.contains("not supported yet"),
        "{message}"
    );
    let unknown = by_author().filter(eq(col("nope"), lit("x")));
    assert!(error_of(&db, &unknown).contains("unknown flat join column"));
    let ordered = by_author().order_by("users.name", jazz::query::OrderDirection::Asc);
    assert!(error_of(&db, &ordered).contains("flat joins with ordering"));
}

/// Deleted rows of either source leave the join.
#[test]
fn flat_join_reads_live_rows_of_every_source() {
    let db = open(schema(None), 1);
    seed(&db);
    block_on(db.delete("posts", post(2), Default::default())).unwrap();
    block_on(db.delete("users", user(2), Default::default())).unwrap();
    assert_eq!(
        tuples(&read(&db, &by_author())),
        [tuple("ada", "a1", post(1))]
    );
}

fn drain(stream: &mut SubscriptionStream) -> Vec<SubscriptionEvent> {
    std::iter::from_fn(|| stream.try_next_event()).collect()
}

/// Maintained results address rows by occurrence: a root that occurs
/// twice is two rows, and a joined row's change adds, removes or replaces
/// exactly its own occurrence.
#[test]
fn flat_join_subscriptions_address_occurrences() {
    let db = open(schema(None), 1);
    seed(&db);
    let prepared = db.prepare_query(&by_author()).unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    drain(&mut stream);

    post_by(&db, 5, "bob", "b2", "bob");
    block_on(db.delete("posts", post(2), Default::default())).unwrap();
    block_on(db.update(
        "posts",
        post(1),
        BTreeMap::from([("title".to_owned(), text("a1, edited"))]),
        Default::default(),
    ))
    .unwrap();
    let (mut added, mut updated, mut removed) = (Vec::new(), Vec::new(), Vec::new());
    for event in drain(&mut stream) {
        if let SubscriptionEvent::Delta {
            added: a,
            updated: u,
            removed: r,
            ..
        } = event
        {
            added.extend(a.iter().map(|row| tuples(std::slice::from_ref(&row.row))));
            updated.extend(u.iter().map(|row| tuples(std::slice::from_ref(&row.row))));
            removed.extend(r.iter().map(|row| {
                (
                    row.row_uuid,
                    row.joined
                        .iter()
                        .map(|joined| joined.row)
                        .collect::<Vec<_>>(),
                )
            }));
        }
    }
    assert_eq!(added.concat(), [tuple("bob", "b2", post(5))]);
    assert_eq!(removed, [(user(1), vec![post(2)])]);
    assert_eq!(updated.concat(), [tuple("ada", "a1, edited", post(1))]);
}

/// Each source is read through its own read policy: a joined row the
/// reader may not see removes its occurrence, not the root's others.
#[test]
fn flat_join_applies_each_sources_read_policy() {
    let db = open(
        schema(Some(session_eq("owner", &["user", "identity", "subject"]))),
        1,
    );
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let me = bob.principal_parts().1;
    insert(&db, "users", user(1), &[("name", "ada")]);
    insert(&db, "users", user(2), &[("name", "bob")]);
    post_by(&db, 1, "ada", "a1", "someone else");
    post_by(&db, 2, "ada", "a2", &me);
    post_by(&db, 3, "bob", "b1", &me);
    let prepared = db.prepare_query(&by_author()).unwrap();
    let rows = block_on(db.all_for_identity(&prepared, ReadOpts::default(), bob)).unwrap();
    assert_eq!(
        tuples(&rows),
        [tuple("ada", "a2", post(2)), tuple("bob", "b1", post(3))]
    );
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..4 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

fn local(db: &Db, table: &str) -> Vec<RowUuid> {
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    block_on(db.all(&db.prepare_query(&Query::from(table)).unwrap(), opts))
        .unwrap()
        .iter()
        .map(CurrentRow::row_uuid)
        .collect()
}

/// A synced flat join ships each occurrence's joined rows with its root,
/// and an occurrence that forms later arrives with its new joined row.
#[test]
fn synced_flat_join_ships_joined_rows() {
    let server = open(schema(None), 0);
    seed(&server);
    let client = open(schema(None), 1);
    let (client_side, server_side) = duplex();
    let _ = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);

    let prepared = client.prepare_query(&by_author()).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        local(&client, "posts"),
        [post(1), post(2), post(3)],
        "only joined posts"
    );
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let rows = block_on(client.all(&prepared, opts.clone())).unwrap();
    assert_eq!(rows.len(), 3);

    post_by(&server, 5, "bob", "b2", "bob");
    pump(&server, &client);
    let rows = block_on(client.all(&prepared, opts)).unwrap();
    assert_eq!(tuples(&rows).last(), Some(&tuple("bob", "b2", post(5))));
}

/// A history source lists each version of its rows: a current row joined
/// to its own history is one occurrence per version, each keyed by it.
#[test]
fn flat_join_with_history_lists_every_version() {
    let db = open(schema(None), 1);
    seed(&db);
    block_on(db.update(
        "posts",
        post(1),
        BTreeMap::from([("title".to_owned(), text("a1, edited"))]),
        Default::default(),
    ))
    .unwrap();
    let query = Query::from("posts")
        .flat_join("history.posts", "posts.id", "history.posts.id")
        .filter(eq(col("posts.title"), lit("a1, edited")));
    let rows = read(&db, &query);
    let titles: Vec<_> = rows
        .iter()
        .map(|row| cell(row, "history.posts.title").unwrap_or_default())
        .collect();
    assert_eq!(titles, ["a1", "a1, edited"]);
    assert!(
        rows.iter()
            .all(|row| row.joined_rows()[0].version.is_some())
    );
    assert_ne!(rows[0].joined_rows(), rows[1].joined_rows());

    // An unbounded history source is refused.
    let unbounded =
        Query::from("posts").flat_join("history.posts", "posts.title", "history.posts.title");
    assert!(error_of(&db, &unbounded).contains("without a bounded row access path"));
}

/// A synced history join ships the versions and their tx ids.
#[test]
fn synced_flat_join_with_history_ships_versions() {
    let server = open(schema(None), 0);
    seed(&server);
    block_on(server.update(
        "posts",
        post(1),
        BTreeMap::from([("title".to_owned(), text("a1, edited"))]),
        Default::default(),
    ))
    .unwrap();
    let client = open(schema(None), 1);
    let (client_side, server_side) = duplex();
    let _ = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    let query = Query::from("posts")
        .flat_join("history.posts", "posts.id", "history.posts.id")
        .filter(eq(col("posts.id"), lit(Value::Uuid(post(1).0))));
    let on_server = read(&server, &query);
    assert_eq!(on_server.len(), 2);
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let rows = block_on(client.all(&prepared, opts)).unwrap();
    assert_eq!(tuples(&rows).len(), 2);

    // A writer's uploaded edit, then a reader's one-shot remote read,
    // bounded by the join alone.
    let writer = open(schema(None), 3);
    let (client_side, server_side) = duplex();
    let _ = block_on(writer.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::for_test_bytes([0xb0; 16]));
    let posts = writer.prepare_query(&Query::from("posts")).unwrap();
    let _held = block_on(writer.subscribe(&posts, ReadOpts::default())).unwrap();
    pump(&server, &writer);
    block_on(writer.update(
        "posts",
        post(3),
        BTreeMap::from([("title".to_owned(), text("b1, edited"))]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &writer);
    let fresh = open(schema(None), 2);
    let (client_side, server_side) = duplex();
    let _ = block_on(fresh.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::for_test_bytes([0xa1; 16]));
    let posts = fresh.prepare_query(&Query::from("posts")).unwrap();
    let _ready = block_on(fresh.subscribe(&posts, ReadOpts::default())).unwrap();
    pump(&server, &fresh);
    let query = Query::from("posts").flat_join("history.posts", "posts.id", "history.posts.id");
    let remote = ReadOpts {
        tier: ReadTier::Remote,
        ..ReadOpts::default()
    };
    let bytes = postcard::to_allocvec(&query).unwrap();
    let read = fresh.all_serialized_query(&bytes, remote, None, None, None);
    let mut read = std::pin::pin!(read);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let rows = loop {
        use std::future::Future;
        if let std::task::Poll::Ready(result) = read.as_mut().poll(&mut context) {
            match result.unwrap() {
                jazz::db::SerializedReadResult::Rows(rows) => break rows,
                other => panic!("{other:?}"),
            }
        }
        pump(&server, &fresh);
    };
    assert_eq!(rows.len(), 6, "four posts, two with two versions");

    // So does a subscription of the same join.
    let prepared = fresh.prepare_query(&query).unwrap();
    let _stream = block_on(fresh.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &fresh);
    let local = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    assert_eq!(block_on(fresh.all(&prepared, local)).unwrap().len(), 6);
}
