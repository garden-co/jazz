//! Row provenance (`$createdAt`, `$updatedAt`, `$createdBy`, `$updatedBy`):
//! engine-maintained per row version, readable, orderable and filterable by
//! name, kept by history, and never writable by the app.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, WriteIdentity, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn db() -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([3; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn body(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".to_owned(), Value::String(text.to_owned()))])
}

fn u64_cell(row: &jazz::db::CurrentRow, name: &str) -> u64 {
    match row.cells().find(|(n, _)| *n == name).unwrap().1 {
        Some(Value::Nullable(Some(v))) => match **v {
            Value::U64(v) => v,
            ref other => panic!("{other:?}"),
        },
        Some(Value::U64(v)) => *v,
        other => panic!("{name}: {other:?}"),
    }
}

#[test]
fn provenance_is_stamped_hidden_by_default_and_selectable() {
    let db = db();
    block_on(db.insert(
        "notes",
        body("a"),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let plain = db.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(db.all(&plain, ReadOpts::default())).unwrap();
    assert!(
        rows[0].cells().all(|(name, _)| !name.starts_with('$')),
        "not in the default projection"
    );

    std::thread::sleep(std::time::Duration::from_millis(5));
    block_on(db.update("notes", row(1), body("b"), Default::default())).unwrap();
    let select = db
        .prepare_query(&Query::from("notes").select([
            "body",
            "$createdAt",
            "$updatedAt",
            "$createdBy.identity.subject",
        ]))
        .unwrap();
    let rows = block_on(db.all(&select, ReadOpts::default())).unwrap();
    let (created, updated) = (
        u64_cell(&rows[0], "$createdAt"),
        u64_cell(&rows[0], "$updatedAt"),
    );
    assert!(created > 1_700_000_000_000, "Unix ms");
    assert!(updated > created, "an update moves $updatedAt only");
}

#[test]
fn provenance_orders_and_filters_and_cannot_be_written() {
    let db = db();
    for n in 1..=3 {
        block_on(db.insert(
            "notes",
            body(&format!("n{n}")),
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(3));
    }
    let newest = db
        .prepare_query(
            &Query::from("notes")
                .order_by("$createdAt", OrderDirection::Desc)
                .limit(1),
        )
        .unwrap();
    assert_eq!(
        block_on(db.all(&newest, ReadOpts::default())).unwrap()[0].row_uuid(),
        row(3)
    );
    // The database's own writes are the system at its node.
    let (_, subject) = AuthorSubject::system_at(NodeUuid::from_bytes([3; 16])).principal_parts();
    let mine = db
        .prepare_query(&Query::from("notes").filter(eq(
            col("$createdBy.identity.subject"),
            lit(Value::String(subject)),
        )))
        .unwrap();
    assert_eq!(
        block_on(db.all(&mine, ReadOpts::default())).unwrap().len(),
        3
    );

    let forged = BTreeMap::from([("$createdAt".to_owned(), Value::U64(1))]);
    let error = block_on(db.update("notes", row(1), forged, Default::default())).unwrap_err();
    assert!(
        error.message.contains("engine-maintained"),
        "{}",
        error.message
    );
    let error = db
        .prepare_query(&Query::from("notes").order_by("$createdBy", OrderDirection::Asc))
        .err();
    let error = match error {
        Some(error) => error,
        None => block_on(
            db.all(
                &db.prepare_query(
                    &Query::from("notes").order_by("$createdBy", OrderDirection::Asc),
                )
                .unwrap(),
                ReadOpts::default(),
            ),
        )
        .unwrap_err(),
    };
    assert!(
        error.message.contains("not supported yet"),
        "{}",
        error.message
    );
}

#[test]
fn history_keeps_each_versions_provenance() {
    let db = db();
    block_on(db.insert(
        "notes",
        body("a"),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let select = db
        .prepare_query(&Query::from("notes").select(["$updatedAt"]))
        .unwrap();
    let first = u64_cell(
        &block_on(db.all(&select, ReadOpts::default())).unwrap()[0],
        "$updatedAt",
    );
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    block_on(db.update("notes", row(1), body("b"), Default::default())).unwrap();
    let at_snapshot = block_on(db.tx_ref(open).all_prepared(&select)).unwrap();
    assert_eq!(
        u64_cell(&at_snapshot[0], "$updatedAt"),
        first,
        "the as-of read sees the old version's stamp"
    );
    db.abandon_transaction_handle(open).unwrap();
}

fn subject_cell(row: &jazz::db::CurrentRow, name: &str) -> String {
    match row.cells().find(|(n, _)| *n == name).unwrap().1 {
        Some(Value::Nullable(Some(v))) => match &**v {
            Value::String(v) => v.clone(),
            other => panic!("{other:?}"),
        },
        Some(Value::String(v)) => v.clone(),
        other => panic!("{name}: {other:?}"),
    }
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}

fn as_user(n: u8, id: RowUuid) -> InsertOptions {
    InsertOptions {
        row_id: Some(id),
        identity: WriteIdentity::Session(user(n)),
        ..Default::default()
    }
}

/// Authors are stored once each, however many versions they write, and
/// still read, filter and order by their identity parts.
#[test]
fn authors_are_interned_once_and_read_back_by_identity() {
    let db = db();
    for n in 0..100u8 {
        block_on(db.insert("notes", body("x"), as_user(1 + n % 2, row(n)))).unwrap();
    }
    for n in 0..100u8 {
        block_on(db.update("notes", row(n), body("y"), Default::default())).unwrap();
    }
    assert_eq!(
        db.interned_authors_for_test(),
        3,
        "two writers and the database"
    );

    let (_, subject) = user(2).principal_parts();
    let by_two = db
        .prepare_query(
            &Query::from("notes")
                .select(["$createdBy.identity.subject", "$updatedBy.identity.subject"])
                .filter(eq(
                    col("$createdBy.identity.subject"),
                    lit(Value::String(subject.clone())),
                )),
        )
        .unwrap();
    let rows = block_on(db.all(&by_two, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 50);
    let (_, system) = AuthorSubject::system_at(NodeUuid::from_bytes([3; 16])).principal_parts();
    for row in &rows {
        assert_eq!(subject_cell(row, "$createdBy.identity.subject"), subject);
        assert_eq!(subject_cell(row, "$updatedBy.identity.subject"), system);
    }
}

/// Author codes are local to each database: a replica whose codes differ
/// from the server's still reads every row's real author.
#[test]
fn author_codes_never_leave_their_database() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let open = |node: u8, author: AuthorSubject| {
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )
    };
    let server = block_on(Db::open_history_complete(open(1, AuthorSubject::SYSTEM))).unwrap();
    let client = block_on(Db::open(open(2, user(7)))).unwrap();
    // Each side interns a different author first, so their codes disagree.
    block_on(server.insert("notes", body("from 5"), as_user(5, row(1)))).unwrap();
    let (up, down) = duplex();
    let _upstream = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, user(7)).unwrap();
    block_on(client.insert(
        "notes",
        body("from 7"),
        InsertOptions {
            row_id: Some(row(2)),
            ..Default::default()
        },
    ))
    .unwrap();
    let select = Query::from("notes").select([
        "body",
        "$createdAt",
        "$updatedAt",
        "$createdBy.identity.subject",
        "$updatedBy.identity.subject",
    ]);
    let prepared = client.prepare_query(&select).unwrap();
    let _live = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    for _ in 0..6 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }

    for (name, db) in [("client", &client), ("server", &server)] {
        let prepared = db.prepare_query(&select).unwrap();
        let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
        let authors: BTreeMap<RowUuid, String> = rows
            .iter()
            .map(|row| {
                // Never updated: the update stamp reads as the create stamp
                // on both sides of the link.
                assert_eq!(
                    u64_cell(row, "$updatedAt"),
                    u64_cell(row, "$createdAt"),
                    "{name}"
                );
                let subject = subject_cell(row, "$createdBy.identity.subject");
                assert_eq!(
                    subject_cell(row, "$updatedBy.identity.subject"),
                    subject,
                    "{name}"
                );
                (row.row_uuid(), subject)
            })
            .collect();
        assert_eq!(
            authors,
            BTreeMap::from([
                (row(1), user(5).principal_parts().1),
                (row(2), user(7).principal_parts().1),
            ]),
            "{name}"
        );
    }
}

/// An author is its account and exact principal: `$createdBy` reads as the
/// whole record, its parts filter, and two principals of one account stay
/// distinct authors.
#[test]
fn author_provenance_carries_the_account() {
    let db = db();
    let account = jazz::account_registry::AccountId(uuid::Uuid::from_bytes([0x0a; 16]));
    let author = AuthorSubject::authenticated("https://issuer.example", "alice")
        .unwrap()
        .with_account(account);
    let other_principal = AuthorSubject::authenticated("https://issuer.example", "alice-2")
        .unwrap()
        .with_account(account);
    for (n, who) in [(1, author), (2, other_principal)] {
        block_on(db.insert(
            "notes",
            body("a"),
            InsertOptions {
                row_id: Some(row(n)),
                identity: WriteIdentity::Session(who),
                ..Default::default()
            },
        ))
        .unwrap();
    }

    let prepared = db
        .prepare_query(
            &Query::from("notes")
                .filter(eq(col("id"), lit(Value::Uuid(row(1).0))))
                .select(["$createdBy", "$createdBy.identity", "$updatedBy"]),
        )
        .unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    let record = |name: &str| -> Vec<Value> {
        match rows[0].cells().find(|(n, _)| *n == name).unwrap().1 {
            Some(Value::Nullable(Some(value))) => match &**value {
                Value::Record(record) => record.to_values().unwrap(),
                other => panic!("{name}: {other:?}"),
            },
            other => panic!("{name}: {other:?}"),
        }
    };
    let identity = |subject: &str| -> Vec<Value> {
        vec![
            Value::String("https://issuer.example".to_owned()),
            Value::String(subject.to_owned()),
        ]
    };
    let created = record("$createdBy");
    assert_eq!(created[0], Value::Uuid(account.0));
    let Value::Record(created_identity) = &created[1] else {
        panic!("{:?}", created[1]);
    };
    assert_eq!(created_identity.to_values().unwrap(), identity("alice"));
    assert_eq!(record("$createdBy.identity"), identity("alice"));
    // Written once: the update author reads as the create author.
    assert_eq!(record("$updatedBy"), created);

    let count = |query: Query| {
        block_on(db.all(&db.prepare_query(&query).unwrap(), ReadOpts::default()))
            .unwrap()
            .len()
    };
    assert_eq!(
        count(
            Query::from("notes").filter(eq(col("$createdBy.account"), lit(Value::Uuid(account.0))))
        ),
        2
    );
    assert_eq!(
        count(Query::from("notes").filter(eq(
            col("$createdBy.identity.subject"),
            lit(Value::String("alice".to_owned()))
        ))),
        1
    );
    assert_eq!(db.interned_authors_for_test(), 2);
}

/// A live query filtered by a provenance part (`$createdBy...`) sees the
/// rows written after it opened: a provenance filter is not a key a changed
/// row can be looked up by, so it must not narrow which commits wake it.
#[test]
fn a_subscription_filtered_by_its_author_sees_new_rows() {
    let db = db();
    let (_, subject) = user(1).principal_parts();
    let mine = db
        .prepare_query(&Query::from("notes").filter(eq(
            col("$createdBy.identity.subject"),
            lit(Value::String(subject)),
        )))
        .unwrap();
    let mut stream = block_on(db.subscribe(&mine, ReadOpts::default())).unwrap();
    block_on(db.insert("notes", body("mine"), as_user(1, row(1)))).unwrap();
    block_on(db.insert("notes", body("theirs"), as_user(2, row(2)))).unwrap();
    let mut seen = Vec::new();
    while let Some(event) = stream.try_next_event() {
        if let jazz::db::SubscriptionEvent::Delta { added, .. } = event {
            seen.extend(added.iter().map(|row| row.row_uuid()));
        }
    }
    assert_eq!(seen, [row(1)]);
}

/// A transaction opened for an identity attributes every write to it, and a
/// write inside a transaction cannot choose another identity.
#[test]
fn a_transaction_for_an_identity_attributes_its_writes_and_fixes_their_identity() {
    let db = db();
    block_on(db.transaction_for_identity(user(1), async |tx| {
        tx.insert(
            "notes",
            body("a"),
            InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        )
        .await
    }))
    .unwrap();
    let staged = block_on(db.mergeable_tx_for_identity(user(2))).unwrap();
    block_on(staged.insert(
        "notes",
        body("b"),
        InsertOptions {
            row_id: Some(row(2)),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(staged.commit()).unwrap();

    let select = db
        .prepare_query(&Query::from("notes").select(["$createdBy.identity.subject"]))
        .unwrap();
    let rows = block_on(db.all(&select, ReadOpts::default())).unwrap();
    let subject_of = |n: u8| {
        let row = rows.iter().find(|r| r.row_uuid() == row(n)).unwrap();
        subject_cell(row, "$createdBy.identity.subject")
    };
    assert_eq!(subject_of(1), user(1).principal_parts().1);
    assert_eq!(subject_of(2), user(2).principal_parts().1);

    let error = block_on(db.transaction_for_identity(user(1), async |tx| {
        tx.insert("notes", body("c"), as_user(2, row(3))).await
    }))
    .unwrap_err();
    assert!(
        error
            .message
            .contains("selected when the transaction is opened"),
        "{error:?}"
    );
    assert_eq!(
        block_on(db.all(&select, ReadOpts::default()))
            .unwrap()
            .len(),
        2
    );
}

/// A code interned for an author inside a transaction that rolls back is
/// undone with it, and SQLite may hand it to the next new author: the
/// first author's later writes must not reuse it.
///
/// Actors: one database; writers 5 and 6.
/// Flow: open tx → 5 writes (code N interned) → abandon (N undone) →
/// 6 writes (N reused for 6) → 5 writes again → 5's row reads as 5's.
#[test]
fn an_author_code_rolled_back_is_not_reused_for_its_author() {
    let db = db();
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    block_on(
        db.tx_ref(open)
            .insert("notes", body("rolled back"), as_user(5, row(1))),
    )
    .unwrap();
    db.abandon_transaction_handle(open).unwrap();
    block_on(db.insert("notes", body("six"), as_user(6, row(2)))).unwrap();
    block_on(db.insert("notes", body("five"), as_user(5, row(3)))).unwrap();

    let select = db
        .prepare_query(&Query::from("notes").select(["body", "$createdBy.identity.subject"]))
        .unwrap();
    let authors: BTreeMap<RowUuid, String> = block_on(db.all(&select, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            (
                row.row_uuid(),
                subject_cell(row, "$createdBy.identity.subject"),
            )
        })
        .collect();
    assert_eq!(
        authors,
        BTreeMap::from([
            (row(2), user(6).principal_parts().1),
            (row(3), user(5).principal_parts().1),
        ])
    );
}

/// A first sync ships a large result in chunks of rows, and the client
/// stores them in one batch that interns each author once: every row
/// arrives, with its own body and author.
///
/// Actors: a server holding 1,100 notes by three writers; a client.
/// Flow: server inserts → client subscribes → ticks → the client's rows
/// equal the server's, row by row.
#[test]
fn a_first_sync_of_many_rows_keeps_every_body_and_author() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let open = |node: u8, author: AuthorSubject| {
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )
    };
    let server = block_on(Db::open_history_complete(open(1, AuthorSubject::SYSTEM))).unwrap();
    let client = block_on(Db::open(open(2, user(7)))).unwrap();
    let many = |n: u16| {
        let mut bytes = [0; 16];
        bytes[..2].copy_from_slice(&n.to_be_bytes());
        RowUuid::from_bytes(bytes)
    };
    for n in 0..1100u16 {
        block_on(server.insert(
            "notes",
            body(&format!("note {n}")),
            InsertOptions {
                row_id: Some(many(n)),
                identity: WriteIdentity::Session(user(1 + (n % 3) as u8)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let (up, down) = duplex();
    let _upstream = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, user(7)).unwrap();
    let select = Query::from("notes").select(["body", "$createdBy.identity.subject"]);
    let prepared = client.prepare_query(&select).unwrap();
    let _live = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    for _ in 0..6 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }

    let read = |db: &Db| -> BTreeMap<RowUuid, (String, String)> {
        let prepared = db.prepare_query(&select).unwrap();
        block_on(db.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .map(|row| {
                (
                    row.row_uuid(),
                    (
                        subject_cell(row, "body"),
                        subject_cell(row, "$createdBy.identity.subject"),
                    ),
                )
            })
            .collect()
    };
    let on_server = read(&server);
    assert_eq!(on_server.len(), 1100);
    assert_eq!(on_server[&many(4)].1, user(2).principal_parts().1);
    assert_eq!(read(&client), on_server);
}
