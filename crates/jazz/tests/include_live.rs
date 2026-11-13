//! A live result with an include sees its include targets change, and a
//! big unbounded view re-checks only the rows that reference the changed
//! target.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema, read_and_allow_all_writes, session_eq};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("senderId", "users")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn id(kind: u8, n: u32) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[12..].copy_from_slice(&n.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn setup(messages: u32) -> Db {
    let schema = schema();
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    let db = block_on(Db::open(config)).unwrap();
    for u in 0..50 {
        db.seed_settled_mergeable_for_bootstrap(
            "users",
            id(1, u),
            AuthorSubject::SYSTEM,
            [("name".to_owned(), Value::String(format!("u{u}")))].into(),
        )
        .unwrap();
    }
    for n in 0..messages {
        let cells: BTreeMap<String, Value> = [
            ("senderId".to_owned(), Value::Uuid(id(1, n % 50).0)),
            ("body".to_owned(), Value::String(format!("m{n}"))),
        ]
        .into();
        db.seed_settled_mergeable_for_bootstrap("messages", id(2, n), AuthorSubject::SYSTEM, cells)
            .unwrap();
    }
    db
}

fn updated(stream: &mut jazz::db::SubscriptionStream) -> usize {
    let mut n = 0;
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta { updated, .. } = event {
            n += updated.len();
        }
    }
    n
}

/// The rows each delta of `stream` delivered so far, opening rows as added
/// and refreshed rows as updated, in delivery order.
fn delivered(stream: &mut jazz::db::SubscriptionStream) -> (Vec<CurrentRow>, Vec<CurrentRow>) {
    let (mut all_added, mut all_updated) = (Vec::new(), Vec::new());
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta { added, updated, .. } = event {
            all_added.extend(added.into_iter().map(|row| row.row));
            all_updated.extend(updated.into_iter().map(|row| row.row));
        }
    }
    (all_added, all_updated)
}

fn ids(rows: &[CurrentRow]) -> Vec<RowUuid> {
    let mut ids: Vec<RowUuid> = rows.iter().map(|row| row.row_uuid()).collect();
    ids.sort();
    ids
}

/// What a host builds an optional include from: the referenced row as
/// `identity` can read it, `None` when the reference is null or the row is
/// hidden from them.
fn include_target(db: &Db, row: &CurrentRow, identity: AuthorSubject) -> Option<CurrentRow> {
    let target = match row.cell("senderId")? {
        Value::Nullable(Some(inner)) => *inner,
        value => value,
    };
    let Value::Uuid(target) = target else {
        return None;
    };
    let target = RowUuid(target);
    let users = db.prepare_query(&Query::from("users")).unwrap();
    block_on(db.all_for_identity(&users, ReadOpts::default(), identity))
        .unwrap()
        .into_iter()
        .find(|user| user.row_uuid() == target)
}

#[test]
fn renaming_a_sender_updates_exactly_their_messages() {
    let db = setup(500);
    let prepared = db
        .prepare_query(&Query::from("messages").include("senderId"))
        .unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    updated(&mut stream);
    block_on(db.update(
        "users",
        id(1, 7),
        [("name".to_owned(), Value::String("renamed".into()))].into(),
        Default::default(),
    ))
    .unwrap();
    block_on(db.tick()).unwrap();
    assert_eq!(updated(&mut stream), 10, "the 10 messages user 7 sent");
}

/// A nullable reference reads back wrapped (`Nullable(Some(uuid))`): renaming
/// its target still updates the messages that point at it, and a message
/// with no sender is left alone.
#[test]
fn renaming_a_sender_updates_messages_through_a_nullable_reference() {
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("users")
                        .column("name", ColumnType::Text)
                        .policies(allow_all_policies()),
                )
                .table(
                    TableSchemaBuilder::new("messages")
                        .nullable_fk_column("senderId", "users")
                        .column("body", ColumnType::Text)
                        .policies(allow_all_policies()),
                )
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    for u in 0..2 {
        db.seed_settled_mergeable_for_bootstrap(
            "users",
            id(1, u),
            AuthorSubject::SYSTEM,
            [("name".to_owned(), Value::String(format!("u{u}")))].into(),
        )
        .unwrap();
    }
    for (n, sender) in [Some(0), Some(1), Some(0), None].into_iter().enumerate() {
        let sender = match sender {
            Some(u) => Value::Uuid(id(1, u).0),
            None => Value::Nullable(None),
        };
        let cells: BTreeMap<String, Value> = [
            ("senderId".to_owned(), sender),
            ("body".to_owned(), Value::String(format!("m{n}"))),
        ]
        .into();
        db.seed_settled_mergeable_for_bootstrap(
            "messages",
            id(2, n as u32),
            AuthorSubject::SYSTEM,
            cells,
        )
        .unwrap();
    }
    let prepared = db
        .prepare_query(&Query::from("messages").include("senderId"))
        .unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    let (opening, _) = delivered(&mut stream);
    let unsent = opening
        .iter()
        .find(|row| row.row_uuid() == id(2, 3))
        .expect("the message with no sender is in the result");
    assert_eq!(unsent.cell("senderId"), Some(Value::Nullable(None)));
    assert!(
        include_target(&db, unsent, AuthorSubject::SYSTEM).is_none(),
        "a null reference includes nothing"
    );
    block_on(db.update(
        "users",
        id(1, 0),
        [("name".to_owned(), Value::String("renamed".into()))].into(),
        Default::default(),
    ))
    .unwrap();
    block_on(db.tick()).unwrap();
    let (_, refreshed) = delivered(&mut stream);
    assert_eq!(
        ids(&refreshed),
        [id(2, 0), id(2, 2)],
        "exactly the 2 messages user 0 sent, not the one with no sender"
    );
    for row in &refreshed {
        let sender = include_target(&db, row, AuthorSubject::SYSTEM).expect("user 0 is readable");
        assert_eq!(sender.row_uuid(), id(1, 0));
        assert_eq!(sender.cell("name"), Some(Value::String("renamed".into())));
    }
}

/// A read policy that hides the include target still applies to it: the
/// reader's live result refreshes through the include they can read, and the
/// sender hidden from them stays out of their reads after it changes.
#[test]
fn a_policy_hidden_include_target_stays_hidden_after_it_changes() {
    let reader = AuthorSubject::for_test_bytes([0x51; 16]);
    let other = AuthorSubject::for_test_bytes([0x52; 16]);
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("users")
                        .column("name", ColumnType::Text)
                        .column("owner", ColumnType::Text)
                        .policies(read_and_allow_all_writes(session_eq(
                            "owner",
                            &["identity", "subject"],
                        ))),
                )
                .table(
                    TableSchemaBuilder::new("messages")
                        .nullable_fk_column("senderId", "users")
                        .column("body", ColumnType::Text)
                        .policies(allow_all_policies()),
                )
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    // User 0 is the reader's own; user 1 is hidden from them.
    for (u, owner) in [(0, reader), (1, other)] {
        db.seed_settled_mergeable_for_bootstrap(
            "users",
            id(1, u),
            AuthorSubject::SYSTEM,
            [
                ("name".to_owned(), Value::String(format!("u{u}"))),
                ("owner".to_owned(), Value::String(owner.principal_parts().1)),
            ]
            .into(),
        )
        .unwrap();
    }
    for n in 0..2 {
        db.seed_settled_mergeable_for_bootstrap(
            "messages",
            id(2, n),
            AuthorSubject::SYSTEM,
            [
                ("senderId".to_owned(), Value::Uuid(id(1, n).0)),
                ("body".to_owned(), Value::String(format!("m{n}"))),
            ]
            .into(),
        )
        .unwrap();
    }
    let prepared = db
        .prepare_query(&Query::from("messages").include("senderId"))
        .unwrap();
    let mut stream =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), reader)).unwrap();
    let (opening, _) = delivered(&mut stream);
    assert_eq!(
        ids(&opening),
        [id(2, 0), id(2, 1)],
        "an optional include keeps every message"
    );
    let hidden = opening
        .iter()
        .find(|row| row.row_uuid() == id(2, 1))
        .unwrap();
    assert!(
        include_target(&db, hidden, reader).is_none(),
        "user 1 is hidden from the reader"
    );

    block_on(db.update(
        "users",
        id(1, 1),
        [("name".to_owned(), Value::String("hidden-renamed".into()))].into(),
        Default::default(),
    ))
    .unwrap();
    block_on(db.tick()).unwrap();
    let (_, refreshed) = delivered(&mut stream);
    // The message may be re-sent (its own cells are readable), but only it:
    // the hidden sender itself never reaches the reader.
    assert!(ids(&refreshed).iter().all(|row| *row == id(2, 1)));
    for row in &refreshed {
        assert_eq!(row.table(), "messages");
        assert!(
            include_target(&db, row, reader).is_none(),
            "user 1 is still hidden"
        );
    }
    let users = db.prepare_query(&Query::from("users")).unwrap();
    let visible = block_on(db.all_for_identity(&users, ReadOpts::default(), reader)).unwrap();
    assert_eq!(
        ids(&visible),
        [id(1, 0)],
        "the rename did not make user 1 readable"
    );

    block_on(db.update(
        "users",
        id(1, 0),
        [("name".to_owned(), Value::String("renamed".into()))].into(),
        Default::default(),
    ))
    .unwrap();
    block_on(db.tick()).unwrap();
    let (_, refreshed) = delivered(&mut stream);
    assert_eq!(
        ids(&refreshed),
        [id(2, 0)],
        "only the reader's own sender's message"
    );
    let sender = include_target(&db, &refreshed[0], reader).expect("user 0 is the reader's");
    assert_eq!(sender.cell("name"), Some(Value::String("renamed".into())));
}

/// Timing probe (run with `--release -- --nocapture`): renaming one sender
/// under a live 10,000-message view with the sender included.
#[test]
fn include_target_change_cost() {
    let db = setup(10_000);
    let prepared = db
        .prepare_query(&Query::from("messages").include("senderId"))
        .unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    updated(&mut stream);
    let mut times = Vec::new();
    for round in 0..20u32 {
        let started = std::time::Instant::now();
        block_on(db.update(
            "users",
            id(1, round),
            [("name".to_owned(), Value::String(format!("r{round}")))].into(),
            Default::default(),
        ))
        .unwrap();
        block_on(db.tick()).unwrap();
        times.push(started.elapsed());
        assert_eq!(updated(&mut stream), 200);
    }
    times.sort();
    eprintln!(
        "include target change under a 10,000-row view: median {:?}",
        times[times.len() / 2]
    );
}
