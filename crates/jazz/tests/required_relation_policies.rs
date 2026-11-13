//! Required relations count only the children the reader can see: a
//! parent whose required relation matches no readable child (or, for
//! cardinality matching, fewer readable children than it references) is
//! left out, the same as a parent whose children don't exist. A child a
//! nested requirement leaves out of the relation doesn't count either.
//!
//! ```
//!   bob ──query messages, require reply──► server
//!                                            │
//!         message ─reply─► note ── note read policy ──✗── bob
//!                                            │
//!                     message left out, not shown with `reply: []`
//! ```

use crate::common;

use std::collections::BTreeMap;
use std::path::Path;

use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, SubscriptionEvent,
    SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, ArraySubqueryRequirement, Include, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

const ME: [&str; 2] = ["identity", "subject"];

/// `notes` are readable by their owner, or by a member a `grants` row
/// names: a policy that reads another table. `messages` reference notes
/// forward (`reply`, `replies`) and notes reference messages back
/// (`message`). `stamps` hang off notes for nested requirements.
fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("messages")
                    .column("body", ColumnType::Text)
                    .fk_column("reply", "notes")
                    .array_fk_column("replies", "notes")
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .nullable_fk_column("message", "messages")
                    .policies(read_and_allow_all_writes(PolicyExpr::or(vec![
                        session_eq("owner", &ME),
                        exists(
                            "grants",
                            vec![outer_eq("note", "id"), session_eq("member", &ME)],
                        ),
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("grants")
                    .fk_column("note", "notes")
                    .column("member", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("stamps")
                    .fk_column("note", "notes")
                    .column("label", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(path: &Path) -> Db {
    block_on(Db::open_at(
        DbConfig::new(
            schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([0; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        path,
        FileOptions::default(),
    ))
    .unwrap()
}

fn id(kind: u8, n: u8) -> RowUuid {
    RowUuid::from_bytes([kind, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn message(n: u8) -> RowUuid {
    id(1, n)
}

fn note(n: u8) -> RowUuid {
    id(2, n)
}

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

fn reference(row: RowUuid) -> Value {
    Value::Uuid(row.0)
}

fn insert(db: &Db, table: &str, row: RowUuid, cells: Vec<(&str, Value)>) {
    let cells: BTreeMap<String, Value> = cells
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
    block_on(db.insert(
        table,
        cells,
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xb0; 16])
}

fn subject(author: AuthorSubject) -> String {
    author.principal_parts().1
}

/// Bob reads note 1 (his own) and note 3 (granted to him), not note 2
/// (alice's).
///
/// | message | reply | replies   | notes pointing back |
/// |---------|-------|-----------|---------------------|
/// | 1       | n1    | [n1, n3]  | n1                  |
/// | 2       | n2    | [n2]      | n2                  |
/// | 3       | n3    | [n1, n2]  | n3                  |
fn seed(db: &Db) {
    let me = subject(bob());
    for n in 1..=3 {
        insert(db, "messages", message(n), messages_cells(n));
    }
    insert(
        db,
        "notes",
        note(1),
        vec![("owner", text(&me)), ("message", reference(message(1)))],
    );
    insert(
        db,
        "notes",
        note(2),
        vec![("owner", text("alice")), ("message", reference(message(2)))],
    );
    insert(
        db,
        "notes",
        note(3),
        vec![("owner", text("alice")), ("message", reference(message(3)))],
    );
    insert(
        db,
        "grants",
        id(3, 1),
        vec![("note", reference(note(3))), ("member", text(&me))],
    );
}

fn messages_cells(n: u8) -> Vec<(&'static str, Value)> {
    let (reply, replies) = match n {
        1 => (note(1), vec![note(1), note(3)]),
        2 => (note(2), vec![note(2)]),
        _ => (note(3), vec![note(1), note(2)]),
    };
    vec![
        ("body", text(&format!("m{n}"))),
        ("reply", reference(reply)),
        (
            "replies",
            Value::Array(replies.into_iter().map(reference).collect()),
        ),
    ]
}

fn reply(requirement: ArraySubqueryRequirement) -> ArraySubquery {
    ArraySubquery::new("reply", "notes", "id", "reply").requirement(requirement)
}

fn replies(requirement: ArraySubqueryRequirement) -> ArraySubquery {
    ArraySubquery::new("replies", "notes", "id", "replies").requirement(requirement)
}

fn back_notes(requirement: ArraySubqueryRequirement) -> ArraySubquery {
    ArraySubquery::new("notes", "notes", "message", "id").requirement(requirement)
}

fn ids(rows: &[CurrentRow]) -> Vec<RowUuid> {
    let mut ids: Vec<RowUuid> = rows.iter().map(CurrentRow::row_uuid).collect();
    ids.sort();
    ids
}

/// The message ids `bob` reads with `relation` attached.
fn read_as_bob(db: &Db, relation: ArraySubquery) -> Vec<RowUuid> {
    let prepared = db
        .prepare_query(&Query::from("messages").array_subquery(relation))
        .unwrap();
    ids(&block_on(db.all_for_identity(&prepared, ReadOpts::default(), bob())).unwrap())
}

/// Each delta's added and removed root ids, in delivery order.
fn deltas(stream: &mut SubscriptionStream) -> (Vec<RowUuid>, Vec<RowUuid>) {
    let (mut added, mut removed) = (Vec::new(), Vec::new());
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta {
            added: a,
            removed: r,
            ..
        } = event
        {
            added.extend(a.iter().map(|row| row.row.row_uuid()));
            removed.extend(r.iter().map(|row| row.row_uuid));
        }
    }
    added.sort();
    removed.sort();
    (added, removed)
}

/// A required forward relation (`AtLeastOne`, what `requireIncludes`
/// sends for a non-nullable reference) keeps only the messages whose
/// referenced note bob can read: his own, and the one granted to him
/// through another table.
#[test]
fn a_required_forward_relation_counts_only_readable_children() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("server.sqlite"));
    seed(&db);
    assert_eq!(
        read_as_bob(&db, reply(ArraySubqueryRequirement::AtLeastOne)),
        [message(1), message(3)]
    );
}

/// A required reverse relation keeps only the messages some readable
/// note points back to.
#[test]
fn a_required_reverse_relation_counts_only_readable_children() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("server.sqlite"));
    seed(&db);
    assert_eq!(
        read_as_bob(&db, back_notes(ArraySubqueryRequirement::AtLeastOne)),
        [message(1), message(3)]
    );
}

/// An array of references, both requirement kinds: `AtLeastOne` keeps a
/// message with any readable element; `MatchCorrelationCardinality`
/// (what `requireIncludes` sends for an array reference) keeps only one
/// whose every element is readable.
#[test]
fn a_required_array_relation_counts_only_readable_elements() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("server.sqlite"));
    seed(&db);
    assert_eq!(
        read_as_bob(&db, replies(ArraySubqueryRequirement::AtLeastOne)),
        [message(1), message(3)],
        "message 2 lists only alice's note"
    );
    assert_eq!(
        read_as_bob(
            &db,
            replies(ArraySubqueryRequirement::MatchCorrelationCardinality)
        ),
        [message(1)],
        "message 3 lists alice's note beside bob's"
    );
}

/// The system reads every note, so the same requirements keep every
/// message whose notes exist: the policy narrows only what a reader sees.
#[test]
fn the_system_reads_every_required_child() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("server.sqlite"));
    seed(&db);
    let all = |relation| {
        let prepared = db
            .prepare_query(&Query::from("messages").array_subquery(relation))
            .unwrap();
        ids(&block_on(db.all(&prepared, ReadOpts::default())).unwrap())
    };
    let every = [message(1), message(2), message(3)];
    assert_eq!(all(reply(ArraySubqueryRequirement::AtLeastOne)), every);
    assert_eq!(
        all(replies(
            ArraySubqueryRequirement::MatchCorrelationCardinality
        )),
        every
    );
}

/// A required child that its own nested requirement leaves out of the
/// relation doesn't satisfy its parent: a message requiring a reply note
/// that must carry a stamp is left out when its note has none, rather
/// than shown with `reply: []`.
///
/// ```
/// message ─requires─► note ─requires─► stamp
///    m1                n1                s1     kept
///    m3                n3                 ✗     note left out → m3 left out
/// ```
#[test]
fn a_nested_requirement_carries_up_to_the_parent() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("server.sqlite"));
    seed(&db);
    insert(
        &db,
        "stamps",
        id(4, 1),
        vec![("note", reference(note(1))), ("label", text("ok"))],
    );
    let stamped = |relation: ArraySubquery| {
        relation.nested(
            ArraySubquery::new("stamps", "stamps", "note", "id")
                .requirement(ArraySubqueryRequirement::AtLeastOne),
        )
    };
    let prepared = db
        .prepare_query(
            &Query::from("messages")
                .array_subquery(stamped(reply(ArraySubqueryRequirement::AtLeastOne))),
        )
        .unwrap();
    assert_eq!(
        ids(&block_on(db.all(&prepared, ReadOpts::default())).unwrap()),
        [message(1)],
        "the system: only note 1 is stamped"
    );
    assert_eq!(
        read_as_bob(&db, stamped(reply(ArraySubqueryRequirement::AtLeastOne))),
        [message(1)],
        "bob: note 3 is readable but unstamped"
    );
    assert_eq!(
        read_as_bob(
            &db,
            stamped(replies(
                ArraySubqueryRequirement::MatchCorrelationCardinality
            ))
        ),
        [],
        "message 1 lists unstamped note 3"
    );
}

/// A live result follows the child's read policy through the table it
/// reads: granting bob alice's note adds the message that requires it,
/// and revoking his own note's ownership removes the message.
///
/// ```
/// bob ──subscribe(messages, require reply)──► server
/// system ──insert grant(n2, bob)──► server ──added m2──► bob
/// system ──update n1.owner = alice──► server ──removed m1──► bob
/// ```
#[test]
fn a_live_required_relation_follows_the_child_policy() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("server.sqlite"));
    seed(&db);
    let prepared = db
        .prepare_query(
            &Query::from("messages").array_subquery(reply(ArraySubqueryRequirement::AtLeastOne)),
        )
        .unwrap();
    let mut stream =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), bob())).unwrap();
    block_on(db.tick()).unwrap();
    assert_eq!(deltas(&mut stream), (vec![message(1), message(3)], vec![]));

    insert(
        &db,
        "grants",
        id(3, 2),
        vec![
            ("note", reference(note(2))),
            ("member", text(&subject(bob()))),
        ],
    );
    block_on(db.tick()).unwrap();
    assert_eq!(deltas(&mut stream), (vec![message(2)], vec![]), "granted");

    block_on(db.update(
        "notes",
        note(1),
        BTreeMap::from([("owner".to_owned(), text("alice"))]),
        Default::default(),
    ))
    .unwrap();
    block_on(db.tick()).unwrap();
    assert_eq!(deltas(&mut stream), (vec![], vec![message(1)]), "revoked");
}

/// A required include (`Include::require_includes`) resolves its target
/// as the reader: a message whose `reply` note bob can't read is left
/// out, as one whose note is gone would be.
#[test]
fn a_required_include_resolves_only_readable_targets() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("server.sqlite"));
    seed(&db);
    let prepared = db
        .prepare_query(
            &Query::from("messages").include_with(Include::new("reply").require_includes()),
        )
        .unwrap();
    assert_eq!(
        ids(&block_on(db.all_for_identity(&prepared, ReadOpts::default(), bob())).unwrap()),
        [message(1), message(3)]
    );
    assert_eq!(
        ids(&block_on(db.all(&prepared, ReadOpts::default())).unwrap()),
        [message(1), message(2), message(3)],
        "the system resolves every note"
    );
}
