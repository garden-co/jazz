//! Correlation-filtered invalidation of array relations (`fanout.rs`): a
//! write to a table that a live result reads only through optional array
//! relations re-runs that result only when the written row is one the
//! result holds at that level, or its old or new correlation value is one
//! the result's parent rows hold there. Everything else the filter cannot
//! decide falls back to a full re-run.
//!
//! Every test here compares each maintained result with a fresh one-shot
//! read (the full re-run path) after every write, on SQLite file storage.
//! The F-numbered gates are the cases a naive filter gets wrong; the
//! randomized differentials at the end cover the rest.

use crate::common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, PreparedQuery, ReadOpts, ReadTier,
    SubscriptionEvent, SubscriptionStream, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::{ArraySubquery, ArraySubqueryRequirement, OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

// ---- harness ---------------------------------------------------------------

pub(crate) type Snapshot = Vec<(RowUuid, Vec<(String, Option<Value>)>)>;

/// A store on a SQLite file in its own scratch directory.
pub(crate) struct Store {
    db: Db,
    _dir: tempfile::TempDir,
}

impl std::ops::Deref for Store {
    type Target = Db;
    fn deref(&self) -> &Db {
        &self.db
    }
}

pub(crate) fn open(schema: JazzSchema, node: u8) -> Store {
    open_as(schema, node, AuthorSubject::SYSTEM)
}

pub(crate) fn open_as(schema: JazzSchema, node: u8, author: AuthorSubject) -> Store {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().join("store.sqlite");
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    let db = block_on(Db::open_at(config, &path, FileOptions::default())).unwrap();
    Store { db, _dir: dir }
}

/// One subscription and the result its deltas maintain.
pub(crate) struct Live {
    pub(crate) name: String,
    query: PreparedQuery,
    identity: Option<AuthorSubject>,
    stream: SubscriptionStream,
    pub(crate) rows: Snapshot,
}

fn owned(row: &jazz::db::CurrentRow) -> Vec<(String, Option<Value>)> {
    row.cells()
        .map(|(name, value)| (name.to_owned(), value.cloned()))
        .collect()
}

pub(crate) fn snapshot(rows: &[jazz::db::CurrentRow]) -> Snapshot {
    rows.iter().map(|r| (r.row_uuid(), owned(r))).collect()
}

pub(crate) fn subscribe(
    db: &Db,
    name: &str,
    query: Query,
    identity: Option<AuthorSubject>,
) -> Live {
    subscribe_with(db, name, query, identity, ReadOpts::default())
}

pub(crate) fn subscribe_with(
    db: &Db,
    name: &str,
    query: Query,
    identity: Option<AuthorSubject>,
    opts: ReadOpts,
) -> Live {
    let query = db.prepare_query(&query).unwrap();
    let stream = match identity {
        Some(author) => block_on(db.subscribe_for_identity(&query, opts, author)),
        None => block_on(db.subscribe(&query, opts)),
    }
    .unwrap();
    Live {
        name: name.to_owned(),
        query,
        identity,
        stream,
        rows: Vec::new(),
    }
}

/// Applies queued deltas to the maintained result.
pub(crate) fn apply(live: &mut Live) {
    while let Some(event) = live.stream.try_next_event() {
        let SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } = event
        else {
            panic!("{}: unexpected event {event:?}", live.name);
        };
        if reset {
            live.rows.clear();
        }
        let moved: BTreeSet<RowUuid> = removed
            .iter()
            .map(|gone| gone.row_uuid)
            .chain(updated.iter().map(|row| row.row_uuid()))
            .collect();
        live.rows.retain(|(id, _)| !moved.contains(id));
        let mut placed: Vec<_> = added.iter().chain(updated.iter()).collect();
        placed.sort_by_key(|row| row.index);
        for row in placed {
            live.rows.insert(row.index, (row.row_uuid(), owned(row)));
        }
    }
}

pub(crate) fn fresh(db: &Db, live: &Live, opts: ReadOpts) -> Snapshot {
    let rows = block_on(db.all_nested_for_test(&live.query, opts, live.identity)).unwrap();
    snapshot(&rows)
}

/// Every maintained result equals a fresh read; returns the fresh results.
pub(crate) fn check(db: &Db, lives: &mut [Live], after: &str) -> Vec<Snapshot> {
    check_with(db, lives, after, ReadOpts::default())
}

pub(crate) fn check_with(
    db: &Db,
    lives: &mut [Live],
    after: &str,
    opts: ReadOpts,
) -> Vec<Snapshot> {
    lives
        .iter_mut()
        .map(|live| {
            apply(live);
            let now = fresh(db, live, opts.clone());
            assert_eq!(
                live.rows, now,
                "{}: the maintained result diverged from a fresh read after {after}",
                live.name
            );
            now
        })
        .collect()
}

pub(crate) fn id(kind: u8, n: usize) -> RowUuid {
    let mut bytes = [kind; 16];
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

pub(crate) fn uuid(row: RowUuid) -> Value {
    Value::Uuid(row.0)
}

pub(crate) fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

pub(crate) fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

pub(crate) fn insert(db: &Db, table: &str, row: RowUuid, pairs: &[(&str, Value)]) {
    block_on(db.insert(
        table,
        cells(pairs),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
}

pub(crate) fn update(db: &Db, table: &str, row: RowUuid, pairs: &[(&str, Value)]) {
    block_on(db.update(table, row, cells(pairs), Default::default())).unwrap();
}

pub(crate) fn delete(db: &Db, table: &str, row: RowUuid) {
    block_on(db.delete(table, row, Default::default())).unwrap();
}

pub(crate) fn user(n: usize) -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x60 + n as u8; 16])
}

pub(crate) fn subject(n: usize) -> Value {
    Value::String(user(n).principal_parts().1)
}

const ME: [&str; 2] = ["identity", "subject"];

// ---- the chat schema of the gates ------------------------------------------

const USERS: u8 = 1;
const CHATS: u8 = 2;
const MESSAGES: u8 = 3;
const MARKS: u8 = 4;
const MEMBERS: u8 = 5;

fn chat_schema(messages_read: Option<PolicyExpr>) -> JazzSchema {
    let messages_policies = match messages_read {
        Some(read) => read_and_allow_all_writes(read),
        None => allow_all_policies(),
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("chats")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chat", "chats")
                    .column("body", ColumnType::Text)
                    .column("sent_at", ColumnType::BigInt)
                    .nullable_fk_column("reply_to", "messages")
                    .nullable_fk_column("author", "users")
                    .array_fk_column("mentions", "users")
                    .nullable_column("sender", ColumnType::Text)
                    .policies(messages_policies),
            )
            .table(
                TableSchemaBuilder::new("read_marks")
                    .fk_column("message", "messages")
                    .column("reader", ColumnType::Text)
                    .column("at", ColumnType::BigInt)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn chat(db: &Db, n: usize, title: &str) {
    insert(db, "chats", id(CHATS, n), &[("title", text(title))]);
}

fn person(db: &Db, n: usize, name: &str) {
    insert(db, "users", id(USERS, n), &[("name", text(name))]);
}

/// Message `n` in chat `chat`, sent at `at`, replying to `reply_to`.
fn message(db: &Db, n: usize, chat: usize, at: i64, reply_to: Option<usize>) {
    insert(
        db,
        "messages",
        id(MESSAGES, n),
        &[
            ("chat", uuid(id(CHATS, chat))),
            ("body", text(&format!("m{n}"))),
            ("sent_at", Value::I64(at)),
            (
                "reply_to",
                reply_to.map_or(Value::Nullable(None), |r| uuid(id(MESSAGES, r))),
            ),
            ("mentions", Value::Array(Vec::new())),
        ],
    );
}

fn in_chat(n: usize) -> jazz::query::Predicate {
    eq(col("chat"), lit(uuid(id(CHATS, n))))
}

fn reply() -> ArraySubquery {
    ArraySubquery::new("reply", "messages", "id", "reply_to")
}

fn replies() -> ArraySubquery {
    ArraySubquery::new("replies", "messages", "reply_to", "id")
        .order_by("sent_at", OrderDirection::Asc)
}

pub(crate) fn reran(db: &Db) -> u64 {
    db.fanout_stats().reran
}

// ---- F1–F7 -----------------------------------------------------------------

/// F1. A required self-relation: a thread row whose reply target is
/// missing is not in the result; it enters when its target arrives, even
/// though the target is in another chat, outside the result, and named by
/// no row the result holds.
///
/// ```
/// alice: messages WHERE chat = 1, reply REQUIRED
/// writer ──insert A(chat 1, reply_to B)──► (A withheld: B missing)
/// writer ──insert B(chat 2)──────────────► A enters with reply [B]
/// ```
#[test]
fn f1_required_self_relation_admits_a_row_when_its_target_arrives() {
    let db = open(chat_schema(None), 1);
    chat(&db, 1, "one");
    chat(&db, 2, "two");
    let mut lives = vec![subscribe(
        &db,
        "thread with required reply",
        Query::from("messages")
            .filter(in_chat(1))
            .array_subquery(reply().requirement(ArraySubqueryRequirement::AtLeastOne)),
        None,
    )];
    check(&db, &mut lives, "subscribe");
    message(&db, 1, 1, 1, Some(2));
    let now = check(&db, &mut lives, "insert A");
    assert!(now[0].is_empty(), "A waits for its target");
    message(&db, 2, 2, 2, None);
    let now = check(&db, &mut lives, "insert B in another chat");
    assert_eq!(now[0].len(), 1, "A entered once B arrived");
    update(
        &db,
        "messages",
        id(MESSAGES, 2),
        &[("body", text("edited"))],
    );
    check(&db, &mut lives, "edit B");
    delete(&db, "messages", id(MESSAGES, 2));
    let now = check(&db, &mut lives, "delete B");
    assert!(now[0].is_empty(), "A leaves with its target");
}

/// F2. The link column is not selected: the result's rows do not carry
/// `reply_to` or `author`, yet a change to the row they name must still
/// reach the nested relation.
///
/// ```
/// alice: messages(body) WHERE chat = 1, reply, author
/// writer ──edit B (named by A.reply_to, unselected)──► A.reply updated
/// writer ──rename U1 (named by A.author)─────────────► A.author updated
/// ```
#[test]
fn f2_an_unselected_link_column_still_correlates() {
    let db = open(chat_schema(None), 1);
    chat(&db, 1, "one");
    chat(&db, 2, "two");
    person(&db, 1, "ann");
    person(&db, 2, "bo");
    message(&db, 2, 2, 1, None);
    message(&db, 3, 2, 1, None);
    message(&db, 1, 1, 2, Some(2));
    update(
        &db,
        "messages",
        id(MESSAGES, 1),
        &[("author", uuid(id(USERS, 1)))],
    );
    let mut lives = vec![
        subscribe(
            &db,
            "thread, link unselected",
            Query::from("messages")
                .select(["body"])
                .filter(in_chat(1))
                .array_subquery(reply()),
            None,
        ),
        subscribe(
            &db,
            "messages with author, link unselected",
            Query::from("messages")
                .select(["body"])
                .filter(in_chat(1))
                .array_subquery(ArraySubquery::new("author", "users", "id", "author")),
            None,
        ),
    ];
    check(&db, &mut lives, "subscribe");
    update(
        &db,
        "messages",
        id(MESSAGES, 2),
        &[("body", text("edited"))],
    );
    check(&db, &mut lives, "edit the reply target");
    update(&db, "users", id(USERS, 1), &[("name", text("anne"))]);
    check(&db, &mut lives, "rename the author");
    update(
        &db,
        "messages",
        id(MESSAGES, 1),
        &[("reply_to", uuid(id(MESSAGES, 3)))],
    );
    check(&db, &mut lives, "retarget the reply");
    update(
        &db,
        "messages",
        id(MESSAGES, 3),
        &[("body", text("new target"))],
    );
    check(&db, &mut lives, "edit the new target");
    update(
        &db,
        "messages",
        id(MESSAGES, 1),
        &[("author", uuid(id(USERS, 2)))],
    );
    update(&db, "users", id(USERS, 2), &[("name", text("bob"))]);
    check(&db, &mut lives, "retarget and rename the author");
}

/// F3. The link column is a list of ids: each element correlates, in a
/// forward relation (`users.id IN message.mentions`) and a reverse one
/// (`message.mentions CONTAINS user.id`).
///
/// ```
/// alice: messages WHERE chat = 1, mentioned (users by mentions[])
/// bob:   users, mentioned_in (messages whose mentions hold the user)
/// writer ──rename U2 (2nd element of A.mentions)──► A.mentioned updated
/// writer ──insert M mentioning U2──────────────────► U2.mentioned_in gains M
/// ```
#[test]
fn f3_a_list_of_ids_correlates_each_element() {
    let db = open(chat_schema(None), 1);
    chat(&db, 1, "one");
    chat(&db, 2, "two");
    for (n, name) in [(1, "ann"), (2, "bo"), (3, "cy")] {
        person(&db, n, name);
    }
    message(&db, 1, 1, 1, None);
    let mentions =
        |list: &[usize]| Value::Array(list.iter().map(|n| uuid(id(USERS, *n))).collect());
    update(
        &db,
        "messages",
        id(MESSAGES, 1),
        &[("mentions", mentions(&[1, 2, 1]))],
    );
    let mut lives = vec![
        subscribe(
            &db,
            "forward list",
            Query::from("messages")
                .filter(in_chat(1))
                .array_subquery(ArraySubquery::new("mentioned", "users", "id", "mentions")),
            None,
        ),
        subscribe(
            &db,
            "reverse list",
            Query::from("users").array_subquery(
                ArraySubquery::new("mentioned_in", "messages", "mentions", "id")
                    .order_by("sent_at", OrderDirection::Asc),
            ),
            None,
        ),
    ];
    check(&db, &mut lives, "subscribe");
    update(&db, "users", id(USERS, 2), &[("name", text("bob"))]);
    check(&db, &mut lives, "rename a later element");
    update(&db, "users", id(USERS, 3), &[("name", text("cyd"))]);
    check(&db, &mut lives, "rename an unmentioned user");
    update(
        &db,
        "messages",
        id(MESSAGES, 1),
        &[("mentions", mentions(&[3]))],
    );
    check(&db, &mut lives, "replace the list");
    update(&db, "users", id(USERS, 3), &[("name", text("cyrus"))]);
    check(&db, &mut lives, "rename the newly mentioned user");
    message(&db, 2, 2, 2, None);
    update(
        &db,
        "messages",
        id(MESSAGES, 2),
        &[("mentions", mentions(&[2]))],
    );
    check(&db, &mut lives, "mention U2 from another chat");
    update(
        &db,
        "messages",
        id(MESSAGES, 2),
        &[("mentions", mentions(&[1, 3]))],
    );
    check(&db, &mut lives, "move the mention");
}

/// F4. An offset page moves when a row on an earlier page leaves: the
/// leaving row is not in the result at all, only before it.
///
/// ```
/// alice: chats, page = messages by chat ORDER BY sent_at OFFSET 1 LIMIT 2
///        [m1] | m2 m3 | m4          (m1 is skipped, not held)
/// writer ──move m1 to chat 2──► | m3 m4 |
/// ```
#[test]
fn f4_an_offset_page_moves_when_an_earlier_row_leaves() {
    let db = open(chat_schema(None), 1);
    chat(&db, 1, "one");
    chat(&db, 2, "two");
    for n in 1..=5 {
        message(&db, n, 1, n as i64, None);
    }
    let mut lives = vec![
        subscribe(
            &db,
            "chat page",
            Query::from("chats")
                .filter(eq(col("title"), lit(text("one"))))
                .array_subquery(
                    ArraySubquery::new("page", "messages", "chat", "id")
                        .order_by("sent_at", OrderDirection::Asc)
                        .offset(1)
                        .limit(2),
                ),
            None,
        ),
        subscribe(
            &db,
            "thread page",
            Query::from("messages")
                .filter(in_chat(1))
                .order_by("sent_at", OrderDirection::Asc)
                .offset(1)
                .limit(2)
                .array_subquery(reply()),
            None,
        ),
    ];
    check(&db, &mut lives, "subscribe");
    update(
        &db,
        "messages",
        id(MESSAGES, 1),
        &[("chat", uuid(id(CHATS, 2)))],
    );
    check(&db, &mut lives, "move the skipped row out");
    delete(&db, "messages", id(MESSAGES, 2));
    check(&db, &mut lives, "delete the skipped row");
    message(&db, 6, 1, 0, None);
    check(&db, &mut lives, "insert before the page");
    update(
        &db,
        "messages",
        id(MESSAGES, 6),
        &[("sent_at", Value::I64(9))],
    );
    check(
        &db,
        &mut lives,
        "move a row from before the page to after it",
    );
}

/// F4, batched: a replica applies two server pushes of one row in one
/// tick, so the row's history holds the intermediate version, not the one
/// the result last saw. A row the result holds (or skipped) still wakes it.
///
/// ```
/// server ──m1: chat 1 → 2──┐
/// server ──m1: chat 2 → 3──┴─► replica tick: old image says chat 2,
///                              the result last saw chat 1
/// ```
#[test]
fn f4_two_pushes_of_one_row_in_one_tick() {
    let schema = chat_schema(None);
    let server = open(schema.clone(), 1);
    let client = open(schema, 2);
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    let pump = || {
        for _ in 0..4 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
    };
    for n in 1..=3 {
        chat(&server, n, &format!("c{n}"));
    }
    for n in 1..=4 {
        message(&server, n, 1, n as i64, None);
    }
    let mut lives = vec![
        subscribe(
            &client,
            "chat 1 with its messages",
            Query::from("chats")
                .filter(eq(col("title"), lit(text("c1"))))
                .array_subquery(
                    ArraySubquery::new("messages", "messages", "chat", "id")
                        .order_by("sent_at", OrderDirection::Asc),
                ),
            None,
        ),
        subscribe(
            &client,
            "chat 1 page",
            Query::from("chats")
                .filter(eq(col("title"), lit(text("c1"))))
                .array_subquery(
                    ArraySubquery::new("page", "messages", "chat", "id")
                        .order_by("sent_at", OrderDirection::Asc)
                        .offset(1)
                        .limit(2),
                ),
            None,
        ),
        // Synced in full so the replica holds every message.
        subscribe(&client, "all messages", Query::from("messages"), None),
        subscribe(
            &client,
            "chat 1's messages, keyed",
            Query::from("messages").filter(in_chat(1)),
            None,
        ),
        subscribe(
            &client,
            "thread 1 with replies, keyed",
            Query::from("messages")
                .filter(in_chat(1))
                .array_subquery(replies()),
            None,
        ),
    ];
    pump();
    let local = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    check_with(&client, &mut lives, "sync", local.clone());
    for row in [1, 2] {
        // Each move goes out in a push of its own; the replica applies
        // both in its next tick.
        update(
            &server,
            "messages",
            id(MESSAGES, row),
            &[("chat", uuid(id(CHATS, 2)))],
        );
        block_on(server.tick()).unwrap();
        update(
            &server,
            "messages",
            id(MESSAGES, row),
            &[("chat", uuid(id(CHATS, 3)))],
        );
        block_on(server.tick()).unwrap();
        block_on(client.tick()).unwrap();
        check_with(
            &client,
            &mut lives,
            &format!("two moves of m{row} in one tick"),
            local.clone(),
        );
        pump();
        check_with(
            &client,
            &mut lives,
            &format!("two moves of m{row} in one tick"),
            local.clone(),
        );
    }
}

/// F5. A reverse link (`inner` is not `id`): a reply in another chat,
/// outside the result and named by none of its rows, enters a held row's
/// `replies` because its `reply_to` names that row.
///
/// ```
/// alice: messages WHERE chat = 1, replies (messages WHERE reply_to = id)
/// writer ──insert R(chat 2, reply_to A)──► A.replies gains R
/// ```
#[test]
fn f5_a_reverse_link_correlates_on_the_child_column() {
    let db = open(chat_schema(None), 1);
    chat(&db, 1, "one");
    chat(&db, 2, "two");
    message(&db, 1, 1, 1, None);
    message(&db, 2, 1, 2, None);
    let mut lives = vec![
        subscribe(
            &db,
            "thread with replies",
            Query::from("messages")
                .filter(in_chat(1))
                .array_subquery(replies()),
            None,
        ),
        subscribe(
            &db,
            "chats with messages",
            Query::from("chats").array_subquery(
                ArraySubquery::new("messages", "messages", "chat", "id")
                    .order_by("sent_at", OrderDirection::Asc),
            ),
            None,
        ),
    ];
    check(&db, &mut lives, "subscribe");
    message(&db, 3, 2, 3, Some(1));
    check(&db, &mut lives, "reply from another chat");
    update(
        &db,
        "messages",
        id(MESSAGES, 3),
        &[("body", text("edited"))],
    );
    check(&db, &mut lives, "edit the reply");
    update(
        &db,
        "messages",
        id(MESSAGES, 3),
        &[("reply_to", uuid(id(MESSAGES, 2)))],
    );
    check(&db, &mut lives, "retarget the reply");
    update(
        &db,
        "messages",
        id(MESSAGES, 3),
        &[("reply_to", Value::Nullable(None))],
    );
    check(&db, &mut lives, "detach the reply");
}

fn posts_v1() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("posts")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("comments")
                    .fk_column("post", "posts")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// v1 with `post` renamed to `old_post`, and a new, unrelated `post`
/// column stored apart from it.
fn posts_v3() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("posts")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("comments")
                    .fk_column("old_post", "posts")
                    .column("body", ColumnType::Text)
                    .nullable_fk_column("post", "posts")
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn comments_lens(from: &JazzSchema, to: &JazzSchema, ops: Vec<LensOp>) -> MigrationLens {
    MigrationLens::new(
        from.version_id(),
        to.version_id(),
        vec![TableLens {
            source_table: "comments".into(),
            target_table: "comments".into(),
            ops,
        }],
    )
    .unwrap()
}

/// F6. Stored names change on a schema reset: after `post` is renamed away
/// and a new `post` is added (a stored column of its own), the live
/// relation `comments WHERE post = posts.id` correlates on the new stored
/// column, so filters are rebuilt with the watches.
///
/// ```
/// v1: comments.post ──rename to old_post, add a new post──► v3
/// alice: posts, comments by post (re-read under each version)
/// writer ──insert comment(post = P1) under v3──► P1.comments gains it
/// ```
#[test]
fn f6_filters_follow_stored_names_across_a_schema_reset() {
    let db = open(posts_v1(), 1);
    let post = |n| id(CHATS, n);
    let comment = |n| id(MESSAGES, n);
    for n in 1..=2 {
        insert(&db, "posts", post(n), &[("title", text(&format!("p{n}")))]);
    }
    insert(
        &db,
        "comments",
        comment(1),
        &[("post", uuid(post(1))), ("body", text("first"))],
    );
    let mut lives = vec![subscribe(
        &db,
        "posts with comments",
        Query::from("posts")
            .array_subquery(ArraySubquery::new("comments", "comments", "post", "id")),
        None,
    )];
    check(&db, &mut lives, "subscribe");
    insert(
        &db,
        "comments",
        comment(2),
        &[("post", uuid(post(2))), ("body", text("second"))],
    );
    check(&db, &mut lives, "insert under v1");
    // One step renames `post` away and adds a new `post`: the live
    // result's query still names a column, now another stored one.
    let rename = comments_lens(
        &posts_v1(),
        &posts_v3(),
        vec![LensOp::RenameColumn {
            from: "post".into(),
            to: "old_post".into(),
        }],
    );
    block_on(db.activate_schema_with_lenses(posts_v3(), &[rename])).unwrap();
    // The live result is re-read under v3, where `post` names another
    // stored column; a fresh subscription under v3 runs beside it.
    lives.push(subscribe(
        &db,
        "posts with comments under v3",
        Query::from("posts")
            .array_subquery(ArraySubquery::new("comments", "comments", "post", "id")),
        None,
    ));
    check(&db, &mut lives, "activate v3");
    // The old column names a post outside the result from here on, so
    // only the new column can tie these comments to a held post.
    update(&db, "comments", comment(1), &[("old_post", uuid(post(9)))]);
    check(&db, &mut lives, "point the old column away");
    insert(
        &db,
        "comments",
        comment(3),
        &[
            ("old_post", uuid(post(9))),
            ("post", uuid(post(1))),
            ("body", text("third")),
        ],
    );
    check(&db, &mut lives, "insert under v3");
    update(&db, "comments", comment(1), &[("post", uuid(post(2)))]);
    check(
        &db,
        &mut lives,
        "link an old comment through the new column",
    );
    update(&db, "comments", comment(3), &[("old_post", uuid(post(1)))]);
    check(&db, &mut lives, "edit the old column only");
}

fn kinds_schema(variants: &[&str]) -> JazzSchema {
    let kind = ColumnType::Enum {
        variants: variants.iter().map(|v| (*v).to_owned()).collect(),
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("labels")
                    .column("kind", kind.clone())
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("items")
                    .column("kind", kind)
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// F6, enum tags: an enum correlation compares stored tags with variant
/// names read back from the result, and tags are numbered per store in
/// declaration order, which a schema change can extend.
///
/// ```
/// alice: labels, items WHERE items.kind = labels.kind
/// writer ──insert item(kind = b)──► label b gains it
/// ```
#[test]
fn f6_enum_correlations_compare_as_stored() {
    let db = open(kinds_schema(&["a", "b"]), 1);
    insert(&db, "labels", id(1, 1), &[("kind", text("a"))]);
    insert(&db, "labels", id(1, 2), &[("kind", text("b"))]);
    let query = || {
        Query::from("labels").array_subquery(ArraySubquery::new("items", "items", "kind", "kind"))
    };
    let mut lives = vec![subscribe(&db, "labels with items", query(), None)];
    check(&db, &mut lives, "subscribe");
    insert(
        &db,
        "items",
        id(2, 1),
        &[("kind", text("b")), ("body", text("x"))],
    );
    check(&db, &mut lives, "insert a b item");
    update(&db, "items", id(2, 1), &[("kind", text("a"))]);
    check(&db, &mut lives, "move it to a");
    let wider = kinds_schema(&["c", "a", "b"]);
    let lens = MigrationLens::new(
        kinds_schema(&["a", "b"]).version_id(),
        wider.version_id(),
        Vec::new(),
    )
    .unwrap();
    block_on(db.activate_schema_with_lenses(wider, &[lens])).unwrap();
    let mut lives = vec![subscribe(
        &db,
        "labels with items, wider enum",
        query(),
        None,
    )];
    insert(&db, "labels", id(1, 3), &[("kind", text("c"))]);
    check(&db, &mut lives, "widen the enum");
    insert(
        &db,
        "items",
        id(2, 2),
        &[("kind", text("c")), ("body", text("y"))],
    );
    check(&db, &mut lives, "insert a c item");
    update(&db, "items", id(2, 1), &[("kind", text("b"))]);
    check(&db, &mut lives, "move the first item to b");
}

/// F7. A read policy that reads other rows of the same table: alice sees
/// her own messages and the ones she replied to. Her reply, written in
/// another chat, makes a message in chat 1 readable, though the reply's
/// own `chat` names no parent the result holds.
///
/// ```
/// alice: chats WHERE title = one, messages by chat
/// bob   ──insert A(chat 1)──────────────────► (hidden from alice)
/// alice ──insert R(chat 2, reply_to A)──────► A becomes readable in chat 1
/// ```
#[test]
fn f7_a_policy_reading_other_rows_of_the_child_table() {
    let mine = session_eq("sender", &ME);
    let replied = exists(
        "messages",
        vec![outer_eq("reply_to", "id"), session_eq("sender", &ME)],
    );
    let db = open(chat_schema(Some(PolicyExpr::Or(vec![mine, replied]))), 1);
    chat(&db, 1, "one");
    chat(&db, 2, "two");
    message(&db, 1, 1, 1, None);
    update(&db, "messages", id(MESSAGES, 1), &[("sender", subject(2))]);
    let alice = Some(user(1));
    let mut lives = vec![
        subscribe(
            &db,
            "alice: chat one with messages",
            Query::from("chats")
                .filter(eq(col("title"), lit(text("one"))))
                .array_subquery(ArraySubquery::new("messages", "messages", "chat", "id")),
            alice,
        ),
        subscribe(
            &db,
            "alice: thread one with replies",
            Query::from("messages")
                .filter(in_chat(1))
                .array_subquery(replies()),
            alice,
        ),
    ];
    let now = check(&db, &mut lives, "subscribe");
    assert!(now[1].is_empty(), "bob's message is hidden from alice");
    message(&db, 2, 2, 2, Some(1));
    update(&db, "messages", id(MESSAGES, 2), &[("sender", subject(1))]);
    let now = check(&db, &mut lives, "alice replies from another chat");
    assert_eq!(
        now[1].len(),
        1,
        "alice now reads the message she replied to"
    );
    update(
        &db,
        "messages",
        id(MESSAGES, 2),
        &[("reply_to", Value::Nullable(None))],
    );
    check(&db, &mut lives, "alice detaches her reply");
}

/// A relation's read policy can read the root table itself: a member's
/// rows nest their chat, readable when the chat has an owner. Another
/// user's membership row, outside the root's `member = alice` key, makes
/// alice's chat readable, so the root keeps no key.
///
/// ```
/// alice: members WHERE member = alice, chat (chats: readable WHERE
///        EXISTS members(chat = id, role = owner))
/// bob ──insert members(chat 1, bob, owner)──► alice's chat 1 appears
/// ```
#[test]
fn a_relation_policy_reading_the_root_table_drops_the_root_key() {
    let owned = exists(
        "members",
        vec![
            outer_eq("chat", "id"),
            jazz::tools::policy_expr::eq("role", "owner"),
        ],
    );
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("chats")
                    .column("title", ColumnType::Text)
                    .policies(read_and_allow_all_writes(owned)),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("chat", "chats")
                    .column("member", ColumnType::Text)
                    .column("role", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let db = open(schema, 1);
    chat(&db, 1, "one");
    let member = |n: usize, who: usize, role: &str| {
        insert(
            &db,
            "members",
            id(MEMBERS, n),
            &[
                ("chat", uuid(id(CHATS, 1))),
                ("member", subject(who)),
                ("role", text(role)),
            ],
        );
    };
    member(1, 1, "guest");
    let mut lives = vec![subscribe(
        &db,
        "alice: her memberships with their chats",
        Query::from("members")
            .filter(eq(col("member"), lit(subject(1))))
            .array_subquery(ArraySubquery::new("chat_row", "chats", "id", "chat")),
        Some(user(1)),
    )];
    check(&db, &mut lives, "subscribe");
    member(2, 2, "owner");
    check(&db, &mut lives, "bob becomes the chat's owner");
    update(&db, "members", id(MEMBERS, 2), &[("role", text("guest"))]);
    check(&db, &mut lives, "bob steps down");
}

/// A session's claims changing re-runs its results in full
/// (`refresh_identity`) and rebuilds their watches and correlations: a
/// relation whose rows are readable by claim shows the newly readable
/// rows, and a later write is matched against what the re-run holds.
///
/// ```
/// alice (region a): chats, messages (readable WHERE region = claims.region)
/// claims ──alice: region b──► chat 1 shows its region-b messages
/// writer ──insert a region-b message in chat 1──► it appears
/// writer ──insert a region-b message in chat 2 (not held)──► skipped
/// ```
#[test]
fn a_claims_change_reruns_and_rebuilds_the_correlations() {
    let by_region = session_eq("region", &["claims", "region"]);
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("chats")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chat", "chats")
                    .column("region", ColumnType::Text)
                    .policies(read_and_allow_all_writes(by_region)),
            )
            .build(),
    );
    let db = open(schema, 1);
    let claims = |region: &str| BTreeMap::from([("\0claims:region".to_owned(), text(region))]);
    let alice = user(1);
    db.set_identity_claims(alice, claims("a"));
    chat(&db, 1, "one");
    chat(&db, 2, "two");
    let note = |n: usize, chat: usize, region: &str| {
        insert(
            &db,
            "messages",
            id(MESSAGES, n),
            &[("chat", uuid(id(CHATS, chat))), ("region", text(region))],
        );
    };
    note(1, 1, "a");
    note(2, 1, "b");
    let mut lives = vec![subscribe(
        &db,
        "alice: chat one with its messages",
        Query::from("chats")
            .filter(eq(col("title"), lit(text("one"))))
            .array_subquery(ArraySubquery::new("messages", "messages", "chat", "id")),
        Some(alice),
    )];
    let opened = check(&db, &mut lives, "subscribe");
    db.set_identity_claims(alice, claims("b"));
    let now = check(&db, &mut lives, "alice moves to region b");
    assert_ne!(opened, now, "region b shows other messages");
    note(3, 1, "b");
    check(&db, &mut lives, "a region-b message in chat 1");
    let before = db.fanout_stats();
    note(4, 2, "b");
    check(&db, &mut lives, "a region-b message in chat 2");
    let after = db.fanout_stats();
    assert_eq!(
        (after.reran, after.uncorrelated - before.uncorrelated),
        (before.reran, 1),
        "chat 2 is not held: the write is skipped by correlation"
    );
}

// ---- the filter engages ----------------------------------------------------

/// A chat list per member whose relations reach a read-marks table, the
/// shape that made one read-mark write re-run every member's chat list:
/// a read mark written on one chat's message re-runs only the lists that
/// hold that message, and every list still equals a fresh read.
///
/// ```
/// alice: chats ─► messages(last) ─► read_marks
/// bob:   chats ─► messages(last) ─► read_marks
/// bob ──read mark on chat 2's last message──► bob's and alice's lists
///                                              re-run only if they hold it
/// ```
#[test]
fn a_child_write_reruns_only_the_results_holding_its_parent() {
    let member = exists(
        "members",
        vec![outer_eq("chat", "id"), session_eq("member", &ME)],
    );
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("chats")
                    .column("title", ColumnType::Text)
                    .policies(read_and_allow_all_writes(member)),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("chat", "chats")
                    .column("member", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chat", "chats")
                    .column("body", ColumnType::Text)
                    .column("sent_at", ColumnType::BigInt)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("read_marks")
                    .fk_column("message", "messages")
                    .column("reader", ColumnType::Text)
                    .column("at", ColumnType::BigInt)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let db = open(schema, 1);
    const PEOPLE: usize = 6;
    for c in 0..PEOPLE {
        chat(&db, c, &format!("c{c}"));
        // Each chat has two members: c and c + 1.
        for (k, m) in [c, (c + 1) % PEOPLE].into_iter().enumerate() {
            insert(
                &db,
                "members",
                id(MEMBERS, c * 2 + k),
                &[("chat", uuid(id(CHATS, c))), ("member", subject(m))],
            );
        }
        for n in 0..3 {
            insert(
                &db,
                "messages",
                id(MESSAGES, c * 10 + n),
                &[
                    ("chat", uuid(id(CHATS, c))),
                    ("body", text("hi")),
                    ("sent_at", Value::I64(n as i64)),
                ],
            );
        }
    }
    let list = || {
        Query::from("chats").array_subquery(
            ArraySubquery::new("last", "messages", "chat", "id")
                .order_by("sent_at", OrderDirection::Desc)
                .limit(1)
                .nested(ArraySubquery::new("marks", "read_marks", "message", "id")),
        )
    };
    let mut lives: Vec<Live> = (0..PEOPLE)
        .map(|u| subscribe(&db, &format!("user {u}'s chat list"), list(), Some(user(u))))
        .collect();
    check(&db, &mut lives, "subscribe");
    let before = reran(&db);
    // The last message of chat 2, held by its two members' lists.
    insert(
        &db,
        "read_marks",
        id(MARKS, 1),
        &[
            ("message", uuid(id(MESSAGES, 22))),
            ("reader", subject(2)),
            ("at", Value::I64(1)),
        ],
    );
    check(&db, &mut lives, "a read mark on a held message");
    let held = reran(&db) - before;
    let before = reran(&db);
    // An earlier message of chat 3: no list holds it.
    insert(
        &db,
        "read_marks",
        id(MARKS, 2),
        &[
            ("message", uuid(id(MESSAGES, 30))),
            ("reader", subject(3)),
            ("at", Value::I64(1)),
        ],
    );
    check(&db, &mut lives, "a read mark on a message no list holds");
    let unheld = reran(&db) - before;
    assert!(
        held <= 2,
        "only chat 2's two members' lists hold its last message: {held} re-ran"
    );
    assert_eq!(unheld, 0, "no list holds message 30: {unheld} re-ran");
    // A new last message in chat 4 reaches its members' lists.
    let before = reran(&db);
    insert(
        &db,
        "messages",
        id(MESSAGES, 49),
        &[
            ("chat", uuid(id(CHATS, 4))),
            ("body", text("new")),
            ("sent_at", Value::I64(9)),
        ],
    );
    check(&db, &mut lives, "a new message");
    assert!(reran(&db) - before <= 2, "only chat 4's members re-ran");
}

/// The thread shape of a self-relation: `messages WHERE chat = C` with the
/// message each one replies to. A message in another chat that no row of
/// a thread names leaves that thread alone.
#[test]
fn a_self_relation_keeps_its_root_key() {
    let db = open(chat_schema(None), 1);
    const THREADS: usize = 5;
    for c in 0..THREADS {
        chat(&db, c, &format!("c{c}"));
        message(&db, c * 10, c, 0, None);
        message(&db, c * 10 + 1, c, 1, Some(c * 10));
    }
    let mut lives: Vec<Live> = (0..THREADS)
        .map(|c| {
            subscribe(
                &db,
                &format!("thread {c}"),
                Query::from("messages")
                    .filter(in_chat(c))
                    .order_by("sent_at", OrderDirection::Asc)
                    .array_subquery(reply()),
                None,
            )
        })
        .collect();
    check(&db, &mut lives, "subscribe");
    let before = db.fanout_stats();
    message(&db, 2, 0, 2, Some(1));
    check(&db, &mut lives, "a message in thread 0");
    let after = db.fanout_stats();
    assert_eq!(
        after.woken - before.woken,
        1,
        "only thread 0 wakes: {before:?} -> {after:?}"
    );
    // A row of thread 1 named by thread 0's reply: both wake.
    let before = db.fanout_stats();
    update(
        &db,
        "messages",
        id(MESSAGES, 2),
        &[("reply_to", uuid(id(MESSAGES, 10)))],
    );
    update(
        &db,
        "messages",
        id(MESSAGES, 10),
        &[("body", text("quoted"))],
    );
    check(&db, &mut lives, "edit a message another thread quotes");
    let after = db.fanout_stats();
    assert!(after.woken - before.woken <= 3, "{before:?} -> {after:?}");
}

// ---- randomized differentials ----------------------------------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % n.max(1)
    }
}

const D_USERS: usize = 4;
const D_CHATS: usize = 4;

/// The differential's chat schema: membership-gated chats and messages,
/// read marks readable by their reader, users readable by everyone.
fn differential_schema() -> JazzSchema {
    let member_of = |column: &str| {
        exists(
            "members",
            vec![outer_eq("chat", column), session_eq("member", &ME)],
        )
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("chats")
                    .column("title", ColumnType::Text)
                    .policies(read_and_allow_all_writes(member_of("id"))),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("chat", "chats")
                    .column("member", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("chat", "chats")
                    .column("body", ColumnType::Text)
                    .column("sent_at", ColumnType::BigInt)
                    .nullable_fk_column("reply_to", "messages")
                    .nullable_fk_column("author", "users")
                    .array_fk_column("mentions", "users")
                    .policies(read_and_allow_all_writes(member_of("chat"))),
            )
            .table(
                TableSchemaBuilder::new("read_marks")
                    .fk_column("message", "messages")
                    .column("reader", ColumnType::Text)
                    .column("at", ColumnType::BigInt)
                    .policies(read_and_allow_all_writes(session_eq("reader", &ME))),
            )
            .build(),
    )
}

fn differential_queries(u: usize) -> Vec<(&'static str, Query)> {
    let chat = u % D_CHATS;
    let author = || ArraySubquery::new("author", "users", "id", "author");
    vec![
        (
            "chat list",
            Query::from("chats")
                .array_subquery(
                    ArraySubquery::new("last", "messages", "chat", "id")
                        .order_by("sent_at", OrderDirection::Desc)
                        .limit(1)
                        .nested(ArraySubquery::new("marks", "read_marks", "message", "id"))
                        .nested(author()),
                )
                .array_subquery(ArraySubquery::new("members", "members", "chat", "id")),
        ),
        (
            "thread",
            Query::from("messages")
                .filter(in_chat(chat))
                .order_by("sent_at", OrderDirection::Asc)
                .array_subquery(reply().nested(author()))
                .array_subquery(replies().limit(2)),
        ),
        (
            "thread page",
            Query::from("messages")
                .filter(in_chat(chat))
                .order_by("sent_at", OrderDirection::Desc)
                .offset(1)
                .limit(3)
                .array_subquery(reply()),
        ),
        (
            "thread, link unselected",
            Query::from("messages")
                .select(["body"])
                .filter(in_chat((chat + 1) % D_CHATS))
                .array_subquery(reply())
                .array_subquery(author()),
        ),
        (
            "required reply",
            Query::from("messages")
                .filter(in_chat(chat))
                .array_subquery(reply().requirement(ArraySubqueryRequirement::AtLeastOne)),
        ),
        (
            "mentions",
            Query::from("users").array_subquery(
                ArraySubquery::new("mentioned_in", "messages", "mentions", "id")
                    .order_by("sent_at", OrderDirection::Asc)
                    .limit(2),
            ),
        ),
        (
            "mentioned users",
            Query::from("messages")
                .filter(in_chat(chat))
                .array_subquery(ArraySubquery::new("mentioned", "users", "id", "mentions")),
        ),
        (
            "chat pages",
            Query::from("chats").array_subquery(
                ArraySubquery::new("page", "messages", "chat", "id")
                    .order_by("sent_at", OrderDirection::Asc)
                    .offset(1)
                    .limit(2)
                    .nested(ArraySubquery::new("marks", "read_marks", "message", "id")),
            ),
        ),
    ]
}

/// The state the random writer draws from.
struct World {
    messages: usize,
    marks: usize,
    members: usize,
}

/// One random write (or a few in one commit); returns what it did.
fn random_write(db: &Db, rng: &mut Rng, world: &mut World, step: usize) -> String {
    let some_message = |rng: &mut Rng, world: &World| id(MESSAGES, rng.next(world.messages));
    let mentions = |rng: &mut Rng| {
        Value::Array(
            (0..rng.next(3))
                .map(|_| uuid(id(USERS, rng.next(D_USERS))))
                .collect(),
        )
    };
    let maybe_message = |rng: &mut Rng, world: &World| {
        if rng.next(3) == 0 || world.messages == 0 {
            Value::Nullable(None)
        } else {
            uuid(id(MESSAGES, rng.next(world.messages)))
        }
    };
    let pick = rng.next(13);
    let result = match pick {
        0 | 1 => {
            let n = world.messages;
            world.messages += 1;
            let cells = [
                ("chat", uuid(id(CHATS, rng.next(D_CHATS)))),
                ("body", text(&format!("m{step}"))),
                ("sent_at", Value::I64(rng.next(40) as i64)),
                ("reply_to", maybe_message(rng, world)),
                ("author", uuid(id(USERS, rng.next(D_USERS)))),
                ("mentions", mentions(rng)),
            ];
            block_on(db.insert(
                "messages",
                self::cells(&cells),
                InsertOptions {
                    row_id: Some(id(MESSAGES, n)),
                    ..Default::default()
                },
            ))
            .map(drop)
        }
        2 => block_on(db.update(
            "messages",
            some_message(rng, world),
            cells(&[("body", text(&format!("e{step}")))]),
            Default::default(),
        ))
        .map(drop),
        3 => block_on(db.update(
            "messages",
            some_message(rng, world),
            cells(&[
                ("chat", uuid(id(CHATS, rng.next(D_CHATS)))),
                ("sent_at", Value::I64(rng.next(40) as i64)),
            ]),
            Default::default(),
        ))
        .map(drop),
        4 => {
            let target = maybe_message(rng, world);
            block_on(db.update(
                "messages",
                some_message(rng, world),
                cells(&[("reply_to", target)]),
                Default::default(),
            ))
            .map(drop)
        }
        5 => {
            let list = mentions(rng);
            block_on(db.update(
                "messages",
                some_message(rng, world),
                cells(&[
                    ("mentions", list),
                    ("author", uuid(id(USERS, rng.next(D_USERS)))),
                ]),
                Default::default(),
            ))
            .map(drop)
        }
        6 => {
            block_on(db.delete("messages", some_message(rng, world), Default::default())).map(drop)
        }
        7 | 8 => {
            let n = world.marks;
            world.marks += 1;
            block_on(db.insert(
                "read_marks",
                cells(&[
                    ("message", uuid(some_message(rng, world))),
                    ("reader", subject(rng.next(D_USERS))),
                    ("at", Value::I64(step as i64)),
                ]),
                InsertOptions {
                    row_id: Some(id(MARKS, n)),
                    ..Default::default()
                },
            ))
            .map(drop)
        }
        9 if world.marks > 0 => block_on(db.update(
            "read_marks",
            id(MARKS, rng.next(world.marks)),
            cells(&[
                ("message", uuid(some_message(rng, world))),
                ("at", Value::I64(step as i64)),
            ]),
            Default::default(),
        ))
        .map(drop),
        10 => block_on(db.update(
            "users",
            id(USERS, rng.next(D_USERS)),
            cells(&[("name", text(&format!("u{step}")))]),
            Default::default(),
        ))
        .map(drop),
        11 => {
            let n = world.members;
            world.members += 1;
            block_on(db.insert(
                "members",
                cells(&[
                    ("chat", uuid(id(CHATS, rng.next(D_CHATS)))),
                    ("member", subject(rng.next(D_USERS))),
                ]),
                InsertOptions {
                    row_id: Some(id(MEMBERS, n)),
                    ..Default::default()
                },
            ))
            .map(drop)
        }
        12 if world.members > 0 => block_on(db.delete(
            "members",
            id(MEMBERS, rng.next(world.members)),
            Default::default(),
        ))
        .map(drop),
        _ => Ok(()),
    };
    // Writes to a deleted row fail; the step then wrote nothing.
    format!(
        "op {pick} ({})",
        if result.is_ok() { "ok" } else { "refused" }
    )
}

fn seed_world(db: &Db) -> World {
    for u in 0..D_USERS {
        person(db, u, &format!("u{u}"));
    }
    let mut world = World {
        messages: 0,
        marks: 0,
        members: 0,
    };
    for c in 0..D_CHATS {
        chat(db, c, &format!("c{c}"));
        for m in [c, (c + 1) % D_USERS] {
            insert(
                db,
                "members",
                id(MEMBERS, world.members),
                &[("chat", uuid(id(CHATS, c))), ("member", subject(m))],
            );
            world.members += 1;
        }
    }
    for n in 0..12 {
        message(db, n, n % D_CHATS, n as i64, (n > 3).then(|| n - 3));
        world.messages += 1;
    }
    world
}

/// Random inserts, edits, moves, deletes, retargets, list changes, read
/// marks and membership changes under every user's chat list, threads,
/// pages and relation shapes: after every commit each maintained result
/// equals a fresh read. The same writes run again with correlation watches
/// off (every relation table watched whole, the full re-run path): both
/// runs maintain the same results, and the filtered run re-runs less.
#[test]
fn correlated_results_match_one_shot_reads_under_random_writes() {
    let mut totals = [(0u64, 0u64); 2];
    for seed in [3, 17, 29, 64, 101] {
        let on = differential_run(seed, true);
        let off = differential_run(seed, false);
        assert_eq!(
            on.0, off.0,
            "seed {seed}: filtered and full re-run results differ"
        );
        for (total, (_, stats)) in totals.iter_mut().zip([&on, &off]) {
            total.0 += stats.reran;
            total.1 += stats.uncorrelated;
        }
    }
    let [(filtered, skipped), (full, none)] = totals;
    assert_eq!(none, 0, "watches off: no correlation skips");
    assert!(skipped > 0, "the filter skipped changes: {skipped}");
    assert!(
        filtered < full,
        "the filter re-runs less: {filtered} re-runs against {full}"
    );
}

/// One seed's random writes; the final results and the fan-out counters.
fn differential_run(seed: u64, correlate: bool) -> (Vec<Snapshot>, jazz::db::FanoutStats) {
    jazz::db::set_correlation_filter_for_test(correlate);
    let db = open(differential_schema(), 1);
    let mut world = seed_world(&db);
    let mut lives: Vec<Live> = (0..D_USERS)
        .flat_map(|u| {
            differential_queries(u)
                .into_iter()
                .map(move |(name, query)| (u, name, query))
        })
        .map(|(u, name, query)| subscribe(&db, &format!("user {u}: {name}"), query, Some(user(u))))
        .collect();
    let mode = if correlate { "filtered" } else { "full" };
    check(&db, &mut lives, &format!("{mode}: subscribe"));
    let mut rng = Rng(seed);
    for step in 0..200 {
        let what = random_write(&db, &mut rng, &mut world, step);
        check(
            &db,
            &mut lives,
            &format!("{mode}: seed {seed} step {step}: {what}"),
        );
    }
    jazz::db::set_correlation_filter_for_test(true);
    let rows = lives.into_iter().map(|live| live.rows).collect();
    (rows, db.fanout_stats())
}

/// A relation's rows are read through their table's read policy, which can
/// read tables neither the root nor its policy reads: those are watched
/// too. Here users nest the messages that mention them, readable to chat
/// members; joining a chat makes its messages appear.
///
/// ```
/// alice: users, mentioned_in (messages: readable WHERE member of chat)
/// writer ──insert members(chat 1, alice)──► chat 1's mentions appear
/// ```
#[test]
fn a_relation_policy_table_is_watched() {
    let db = open(differential_schema(), 1);
    for u in 0..2 {
        person(&db, u, &format!("u{u}"));
    }
    chat(&db, 1, "one");
    message(&db, 1, 1, 1, None);
    update(
        &db,
        "messages",
        id(MESSAGES, 1),
        &[("mentions", Value::Array(vec![uuid(id(USERS, 0))]))],
    );
    let mut lives = vec![subscribe(
        &db,
        "alice: users with their mentions",
        Query::from("users").array_subquery(ArraySubquery::new(
            "mentioned_in",
            "messages",
            "mentions",
            "id",
        )),
        Some(user(1)),
    )];
    check(&db, &mut lives, "subscribe");
    insert(
        &db,
        "members",
        id(MEMBERS, 1),
        &[("chat", uuid(id(CHATS, 1))), ("member", subject(1))],
    );
    check(&db, &mut lives, "alice joins chat 1");
    delete(&db, "members", id(MEMBERS, 1));
    check(&db, &mut lives, "alice leaves chat 1");
}

/// How the leaf replica reaches the authority.
#[derive(Clone, Copy, Debug)]
enum Topology {
    /// Connected to the authority, served as SYSTEM.
    Direct,
    /// Connected to the authority, served as user 0: the authority's
    /// results for it are identity-bound, read-policy-checked groups.
    DirectAs,
    /// Through a relay that is served as SYSTEM and serves the leaf as
    /// user 0: the relay's results for it are identity-bound groups over
    /// the rows it holds.
    RelayAs,
}

/// The same writes reaching a replica in batches: the authority commits
/// one to four writes, often to the same row, and the replica applies them
/// in one tick, directly or through a relay, as SYSTEM or as a user. Its
/// maintained results equal fresh local reads after each batch, and its
/// relation results equal what the authority reads for the same identity.
#[test]
fn correlated_results_match_one_shot_reads_on_a_replica_under_batched_pushes() {
    for (topology, seeds) in [
        (Topology::Direct, &[5, 13][..]),
        (Topology::DirectAs, &[31, 47][..]),
        (Topology::RelayAs, &[59, 71][..]),
    ] {
        for &seed in seeds {
            replica_run(topology, seed);
        }
    }
}

fn replica_run(topology: Topology, seed: u64) {
    let (served_as, identity) = match topology {
        Topology::Direct => (AuthorSubject::SYSTEM, None),
        Topology::DirectAs | Topology::RelayAs => (user(0), Some(user(0))),
    };
    let server = open(differential_schema(), 1);
    let relay = matches!(topology, Topology::RelayAs).then(|| open(differential_schema(), 3));
    let client = open_as(differential_schema(), 2, served_as);
    let mut links = Vec::new();
    match &relay {
        None => {
            let (client_side, server_side) = duplex();
            links.push(block_on(client.connect_upstream(client_side)));
            server.accept_subscriber(server_side, served_as).unwrap();
        }
        Some(relay) => {
            let (relay_side, server_side) = duplex();
            links.push(block_on(relay.connect_upstream(relay_side)));
            server
                .accept_subscriber(server_side, AuthorSubject::SYSTEM)
                .unwrap();
            relay.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
            let (client_side, relay_down) = duplex();
            links.push(block_on(client.connect_upstream(client_side)));
            relay.accept_subscriber(relay_down, served_as).unwrap();
        }
    }
    let pump = || {
        for _ in 0..5 {
            block_on(client.tick()).unwrap();
            if let Some(relay) = &relay {
                block_on(relay.tick()).unwrap();
            }
            block_on(server.tick()).unwrap();
            if let Some(relay) = &relay {
                block_on(relay.tick()).unwrap();
            }
        }
        block_on(client.tick()).unwrap();
    };
    let mut world = seed_world(&server);
    let queries = differential_queries(0);
    let mut lives: Vec<Live> = queries
        .iter()
        .map(|(name, query)| subscribe(&client, name, query.clone(), None))
        .collect();
    // Every row the leaf may read synced, so its relations read what the
    // authority lets it read.
    for table in ["users", "chats", "members", "messages", "read_marks"] {
        lives.push(subscribe(&client, table, Query::from(table), None));
    }
    let local = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let authority: Vec<PreparedQuery> = queries
        .iter()
        .map(|(_, query)| server.prepare_query(query).unwrap())
        .collect();
    let converged = |lives: &[Live], after: &str| {
        for (live, query) in lives.iter().zip(&authority) {
            // A required relation's requirement is checked without the
            // child's read policy (`array_requirements`), so the authority
            // keeps a row whose required child the identity cannot read,
            // which a leaf holding only readable rows leaves out.
            if identity.is_some() && live.name == "required reply" {
                continue;
            }
            let truth = snapshot(
                &block_on(server.all_nested_for_test(query, ReadOpts::default(), identity))
                    .unwrap(),
            );
            assert_eq!(
                live.rows, truth,
                "{topology:?} {}: the leaf's result differs from the authority's read after {after}",
                live.name
            );
        }
    };
    pump();
    check_with(&client, &mut lives, "sync", local.clone());
    converged(&lives, "sync");
    let mut rng = Rng(seed);
    for step in 0..80 {
        let mut what = Vec::new();
        for _ in 0..1 + rng.next(4) {
            what.push(random_write(&server, &mut rng, &mut world, step));
        }
        pump();
        let after = format!("{topology:?} seed {seed} step {step}: {what:?}");
        check_with(&client, &mut lives, &after, local.clone());
        converged(&lives, &after);
    }
    // The authority's identity-bound results for the leaf skipped rows by
    // correlation: the run exercises the filter, not only full re-runs. (A
    // relay's mirrors of the leaf's reads are re-woken by the parent rows
    // its upstream re-ships with every changed result, so there the filter
    // rarely gets to decide.)
    if matches!(topology, Topology::DirectAs) {
        let stats = server.fanout_stats();
        assert!(
            stats.uncorrelated > 0,
            "{topology:?} seed {seed}: {stats:?}"
        );
    }
}
