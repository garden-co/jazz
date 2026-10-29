//! Old sync and settlement reports rechecked against the SQLite engine:
//! each test pins the behaviour its issue asked for.
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, RemoteLinkHint, SubscriptionEvent,
    SubscriptionStream, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, Operation, PolicyExpr, PolicyValue, SchemaBuilder,
    TablePolicies, TableSchemaBuilder,
};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema, exists, outer_eq, session_eq};

const ME: [&str; 3] = ["user", "identity", "subject"];

fn me(column: &str) -> PolicyExpr {
    session_eq(column, &ME)
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xb0; 16])
}

fn subject(author: AuthorSubject) -> Value {
    Value::String(author.principal_parts().1)
}

fn row(kind: u8, n: u16) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[14..].copy_from_slice(&n.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

fn cells(pairs: Vec<(&str, Value)>) -> BTreeMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn config(schema: JazzSchema, node: u8, author: AuthorSubject) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn authority(schema: JazzSchema) -> Db {
    block_on(Db::open_history_complete(config(
        schema,
        0xee,
        AuthorSubject::SYSTEM,
    )))
    .unwrap()
}

fn connect(db: &Db, server: &Db, author: AuthorSubject) -> Upstream {
    let (client_side, server_side) = duplex_transport::duplex();
    let upstream = block_on(db.connect_upstream(client_side));
    server.accept_subscriber(server_side, author);
    db.set_remote_link_hint(RemoteLinkHint::Live);
    upstream
}

/// A replica connected to `server` as `author`.
fn replica(schema: JazzSchema, server: &Db, node: u8, author: AuthorSubject) -> Db {
    let db = block_on(Db::open(config(schema, node, author))).unwrap();
    connect(&db, server, author);
    db
}

fn seed(server: &Db, table: &str, id: RowUuid, pairs: Vec<(&str, Value)>) {
    block_on(server.insert(
        table,
        cells(pairs),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(server.tick()).unwrap();
}

/// Poll `future`, ticking `dbs` between polls, until it completes.
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..400 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn pump(dbs: &[&Db]) {
    for _ in 0..8 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}

fn settle(db: &Db, server: &Db, tx: u64) -> Result<u64, jazz::db::Error> {
    drive(
        &[db, server, db],
        db.wait_for_transaction(tx, DurabilityTier::Global),
    )
}

fn ids(db: &Db, query: Query, tier: ReadTier) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&query).unwrap();
    let mut ids: Vec<RowUuid> = block_on(db.all(&prepared, ReadOpts::for_read_tier(tier)))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    ids.sort();
    ids
}

fn subscribe(db: &Db, query: Query) -> SubscriptionStream {
    let prepared = db.prepare_query(&query).unwrap();
    block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap()
}

/// The deltas a subscription delivered since the last call: rows added and
/// removed, failing on a rejection.
fn deltas(stream: &mut SubscriptionStream) -> (Vec<RowUuid>, Vec<RowUuid>) {
    let (mut added, mut removed) = (Vec::new(), Vec::new());
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
            SubscriptionEvent::Rejected { reason } => panic!("subscription rejected: {reason}"),
            SubscriptionEvent::Closed => {}
        }
    }
    added.sort();
    removed.sort();
    (added, removed)
}

// ---- writes whose policies read the row or the session ----------------------

/// Presence rows anyone may write and only their creator may delete;
/// invitations their recipient may accept.
fn wequencer() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("presence")
                    .column("name", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_delete(session_eq("$createdBy.identity.subject", &ME)),
                    ),
            )
            .table(
                TableSchemaBuilder::new("invitations")
                    .column("recipient", ColumnType::Text)
                    .column("status", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(me("recipient"))
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(me("recipient")), me("recipient")),
                    ),
            )
            .build(),
    )
}

/// #1913: a delete its creator may make settles at the authority, and
/// another user's delete of the same row is refused.
#[test]
fn a_delete_by_the_rows_creator_settles() {
    let server = authority(wequencer());
    let alice_db = replica(wequencer(), &server, 0xa1, alice());
    let bob_db = replica(wequencer(), &server, 0xb0, bob());
    let insert = block_on(alice_db.insert(
        "presence",
        cells(vec![("name", text("alice"))]),
        InsertOptions::default(),
    ))
    .unwrap();
    let presence = insert.row_uuid();
    settle(&alice_db, &server, insert.mergeable_tx_id()).unwrap();
    pump(&[&bob_db, &server, &bob_db]);
    let _bob_view = subscribe(&bob_db, Query::from("presence"));
    pump(&[&bob_db, &server, &bob_db]);

    let refused = block_on(bob_db.delete("presence", presence, Default::default())).unwrap();
    assert!(settle(&bob_db, &server, refused.mergeable_tx_id()).is_err());
    let deleted = block_on(alice_db.delete("presence", presence, Default::default())).unwrap();
    settle(&alice_db, &server, deleted.mergeable_tx_id()).unwrap();
    assert_eq!(
        ids(&server, Query::from("presence"), ReadTier::LocalOnly),
        []
    );
}

/// #1907: a recipient's update of an invitation they observed settles.
#[test]
fn a_recipients_acceptance_of_an_invitation_settles() {
    let server = authority(wequencer());
    let invitation = row(1, 1);
    seed(
        &server,
        "invitations",
        invitation,
        vec![("recipient", subject(bob())), ("status", text("pending"))],
    );
    let bob_db = replica(wequencer(), &server, 0xb0, bob());
    let mut view = subscribe(&bob_db, Query::from("invitations"));
    pump(&[&bob_db, &server, &bob_db]);
    assert_eq!(deltas(&mut view).0, [invitation]);
    let accept = block_on(bob_db.update(
        "invitations",
        invitation,
        cells(vec![("status", text("accepted"))]),
        Default::default(),
    ))
    .unwrap();
    settle(&bob_db, &server, accept.mergeable_tx_id()).unwrap();
    let accepted = Query::from("invitations").filter(eq(col("status"), lit(text("accepted"))));
    assert_eq!(ids(&server, accepted, ReadTier::LocalOnly), [invitation]);
}

/// #3450: two connections of one user with different claims each write
/// under their own claims, though their commits reach the authority in the
/// same tick.
#[test]
fn each_connection_writes_under_its_own_claims() {
    let editors_only = PolicyExpr::Cmp {
        column: "author_role".to_owned(),
        op: jazz::tools::CmpOp::Eq,
        value: PolicyValue::SessionRef(vec!["claims".to_owned(), "role".to_owned()]),
    };
    let schema = || {
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("author_role", ColumnType::Text)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::True)
                                .with_insert(editors_only.clone()),
                        ),
                )
                .build(),
        )
    };
    let server = authority(schema());
    let device = |node: u8, role: &str| {
        let db = block_on(Db::open(config(schema(), node, alice()))).unwrap();
        let (client_side, server_side) = duplex_transport::duplex();
        block_on(db.connect_upstream(client_side));
        server.accept_subscriber_with_claims(
            server_side,
            alice(),
            Some(BTreeMap::from([(
                jazz::query::provider_claim_key("role"),
                text(role),
            )])),
        );
        db
    };
    let editor = device(0xa1, "editor");
    let viewer = device(0xa2, "viewer");
    pump(&[&editor, &viewer, &server, &editor, &viewer]);
    // Each claims to be an editor; only the editor's connection is one.
    let write = |db: &Db| {
        block_on(db.insert(
            "notes",
            cells(vec![("author_role", text("editor"))]),
            InsertOptions::default(),
        ))
        .unwrap()
        .mergeable_tx_id()
    };
    let (by_editor, by_viewer) = (write(&editor), write(&viewer));
    let all = [&editor, &viewer, &server, &editor, &viewer];
    let wait = |db: &Db, tx| drive(&all, db.wait_for_transaction(tx, DurabilityTier::Global));
    assert!(wait(&editor, by_editor).is_ok());
    assert!(wait(&viewer, by_viewer).is_err());
    assert_eq!(
        ids(&server, Query::from("notes"), ReadTier::LocalOnly).len(),
        1
    );
}

// ---- retraction when access is lost -----------------------------------------

/// Docs readable through their group, which its members can read.
fn grouped_docs() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("groups")
                    .column("name", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(exists(
                                "memberships",
                                vec![outer_eq("group", "id"), me("user_id")],
                            ))
                            .with_insert(PolicyExpr::True),
                    ),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("group", "groups")
                    .column("user_id", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("docs")
                    .fk_column("group", "groups")
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::inherits(Operation::Select, "group"))
                            .with_insert(PolicyExpr::True),
                    ),
            )
            .build(),
    )
}

/// #1764: removing a user from a group retracts each document inherited
/// through it from their live subscription exactly once, and leaves the
/// documents of their other group alone.
#[test]
fn losing_a_membership_retracts_each_inherited_doc_once() {
    let server = authority(grouped_docs());
    for g in 1..=2 {
        seed(&server, "groups", row(1, g), vec![("name", text("g"))]);
        seed(
            &server,
            "memberships",
            row(2, g),
            vec![
                ("group", Value::Uuid(row(1, g).0)),
                ("user_id", subject(alice())),
            ],
        );
        for d in 0..3 {
            seed(
                &server,
                "docs",
                row(3, g * 10 + d),
                vec![("group", Value::Uuid(row(1, g).0)), ("title", text("d"))],
            );
        }
    }
    let alice_db = replica(grouped_docs(), &server, 0xa1, alice());
    let mut docs = subscribe(&alice_db, Query::from("docs"));
    pump(&[&alice_db, &server, &alice_db]);
    assert_eq!(deltas(&mut docs).0.len(), 6);

    block_on(server.delete("memberships", row(2, 1), Default::default())).unwrap();
    pump(&[&alice_db, &server, &alice_db]);
    let (added, removed) = deltas(&mut docs);
    assert_eq!(added, []);
    assert_eq!(removed, [row(3, 10), row(3, 11), row(3, 12)]);
    assert_eq!(
        ids(&alice_db, Query::from("docs"), ReadTier::LocalOnly),
        [row(3, 20), row(3, 21), row(3, 22)]
    );
}

// ---- offline reads after local writes ---------------------------------------

/// #2691: offline, a filtered read inside a transaction still returns every
/// matching row after one of them was updated in place locally.
#[test]
fn an_offline_filtered_read_keeps_a_row_updated_in_place() {
    let schema = || {
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("orders")
                        .column("label", ColumnType::Text)
                        .policies(allow_all_policies()),
                )
                .table(
                    TableSchemaBuilder::new("items")
                        .fk_column("order", "orders")
                        .column("quantity", ColumnType::Integer)
                        .column("subtotal", ColumnType::Double)
                        .policies(allow_all_policies()),
                )
                .build(),
        )
    };
    let server = authority(schema());
    seed(&server, "orders", row(1, 1), vec![("label", text("o"))]);
    for n in 0..10 {
        seed(
            &server,
            "items",
            row(2, n),
            vec![
                ("order", Value::Uuid(row(1, 1).0)),
                ("quantity", Value::I32(1)),
                ("subtotal", Value::F64(2.5)),
            ],
        );
    }
    let client = block_on(Db::open(config(schema(), 0xa1, alice()))).unwrap();
    let upstream = connect(&client, &server, alice());
    let of_order = || Query::from("items").filter(eq(col("order"), lit(Value::Uuid(row(1, 1).0))));
    let _items = subscribe(&client, of_order());
    pump(&[&client, &server, &client]);
    assert_eq!(ids(&client, of_order(), ReadTier::LocalOnly).len(), 10);

    assert!(client.detach_connection(&upstream));
    client.set_remote_link_hint(RemoteLinkHint::Failed);
    block_on(client.update(
        "items",
        row(2, 3),
        cells(vec![
            ("quantity", Value::I32(4)),
            ("subtotal", Value::F64(10.0)),
        ]),
        Default::default(),
    ))
    .unwrap();
    let open = OpenTransactionId::new();
    block_on(client.begin_mergeable(open)).unwrap();
    let prepared = client.prepare_query(&of_order()).unwrap();
    let in_tx = block_on(client.tx_ref(open).all_prepared(&prepared)).unwrap();
    assert_eq!(in_tx.len(), 10);
    client.abandon_transaction_handle(open).unwrap();
    for tier in [ReadTier::LocalFirst, ReadTier::LocalOnly] {
        assert_eq!(ids(&client, of_order(), tier).len(), 10, "{tier:?}");
    }
}

// ---- large transactions and restarts ----------------------------------------

fn notes() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// #2125: one transaction of 10,000 rows syncs to the authority whole.
#[test]
fn a_ten_thousand_row_transaction_syncs_whole() {
    let server = authority(notes());
    let client = replica(notes(), &server, 0xa1, alice());
    let tx = block_on(client.mergeable_tx()).unwrap();
    for n in 0..10_000u16 {
        block_on(tx.insert(
            "notes",
            cells(vec![("body", text(&format!("note {n} with some text")))]),
            InsertOptions {
                row_id: Some(row(5, n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let receipt = block_on(tx.commit()).unwrap();
    settle(&client, &server, receipt.0).unwrap();
    assert_eq!(
        ids(&server, Query::from("notes"), ReadTier::LocalOnly).len(),
        10_000
    );
}

/// #3350: a subscription survives the server restarting on the same store
/// while another client wrote offline: no rejection, and the write that was
/// pending across the restart reaches it.
#[test]
fn a_subscription_survives_a_server_restart_and_receives_the_pending_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("server.sqlite");
    let start = || {
        let db = block_on(Db::open_at(
            config(notes(), 0xee, AuthorSubject::SYSTEM),
            &path,
            jazz::db::FileOptions::default(),
        ))
        .unwrap();
        db.set_authority().unwrap();
        db
    };
    let server = start();
    let reader = block_on(Db::open(config(notes(), 0xb0, bob()))).unwrap();
    let writer = block_on(Db::open(config(notes(), 0xa1, alice()))).unwrap();
    let reader_link = connect(&reader, &server, bob());
    let writer_link = connect(&writer, &server, alice());
    let mut view = subscribe(&reader, Query::from("notes"));
    let first = block_on(writer.insert(
        "notes",
        cells(vec![("body", text("before"))]),
        InsertOptions::default(),
    ))
    .unwrap();
    settle(&writer, &server, first.mergeable_tx_id()).unwrap();
    pump(&[&reader, &server, &reader]);
    assert_eq!(deltas(&mut view).0, [first.row_uuid()]);

    // The server stops; the writer writes while it is down.
    assert!(reader.detach_connection(&reader_link));
    assert!(writer.detach_connection(&writer_link));
    drop(server);
    let pending = block_on(writer.insert(
        "notes",
        cells(vec![("body", text("during"))]),
        InsertOptions::default(),
    ))
    .unwrap();

    let server = start();
    connect(&reader, &server, bob());
    connect(&writer, &server, alice());
    settle(&writer, &server, pending.mergeable_tx_id()).unwrap();
    pump(&[&reader, &writer, &server, &reader]);
    let (added, removed) = deltas(&mut view);
    assert_eq!(added, [pending.row_uuid()]);
    assert_eq!(removed, []);
}

/// #3760, #3762: a write, including a trusted session's upsert that merges
/// a read-authorized preimage, completes in its first poll while another
/// operation on the same database is suspended, so a synchronous caller
/// never spins in `block_on`, and the suspended operation still finishes.
#[test]
fn writes_complete_in_one_poll_while_another_operation_waits() {
    let schema = || {
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("title", ColumnType::Text)
                        .policies(allow_all_policies()),
                )
                .build(),
        )
    };
    let server = authority(schema());
    let client = replica(schema(), &server, 0xa1, alice());
    let first = block_on(client.insert(
        "notes",
        cells(vec![("title", text("first"))]),
        InsertOptions {
            row_id: Some(row(1, 1)),
            ..Default::default()
        },
    ))
    .unwrap();

    let mut context = Context::from_waker(Waker::noop());
    let mut global =
        pin!(client.wait_for_transaction(first.mergeable_tx_id(), DurabilityTier::Global));
    assert!(global.as_mut().poll(&mut context).is_pending());

    fn once<T>(future: impl Future<Output = T>) -> T {
        let mut context = Context::from_waker(Waker::noop());
        match pin!(future).poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("a write waited on a suspended operation"),
        }
    }
    let as_alice = jazz::db::WriteIdentity::Session(alice());
    once(client.insert(
        "notes",
        cells(vec![("title", text("second"))]),
        InsertOptions {
            row_id: Some(row(1, 2)),
            ..Default::default()
        },
    ))
    .unwrap();
    once(client.update(
        "notes",
        row(1, 1),
        cells(vec![("title", text("renamed"))]),
        Default::default(),
    ))
    .unwrap();
    once(server.upsert(
        "notes",
        row(1, 3),
        cells(vec![("title", text("served"))]),
        jazz::db::UpsertOptions {
            identity: as_alice.clone(),
            ..Default::default()
        },
    ))
    .unwrap();
    once(server.upsert(
        "notes",
        row(1, 3),
        cells(vec![("title", text("merged"))]),
        jazz::db::UpsertOptions {
            identity: as_alice,
            ..Default::default()
        },
    ))
    .unwrap();
    once(client.delete("notes", row(1, 2), Default::default())).unwrap();
    once(client.restore("notes", row(1, 2), None, Default::default())).unwrap();

    drive(&[&client, &server], global).unwrap();
    let notes = ids(&server, Query::from("notes"), ReadTier::LocalOnly);
    assert_eq!(notes.len(), 3);
}

// ---- JSON Schema at the authority -------------------------------------------

fn json_docs(schema: Option<serde_json::Value>) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("payload", ColumnType::Json { schema })
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// A JSON object of about `len` bytes: `{"state":<state>,"pad":"aaa…"}`.
fn json_doc(state: &str, len: usize) -> Value {
    text(&format!(
        r#"{{"state":{state},"pad":"{}"}}"#,
        "a".repeat(len)
    ))
}

/// #3553, #3924: the authority checks every uploaded JSON value against the
/// column's schema, whatever the writer's own replica checked, and a value
/// large enough to travel in chunks is checked whole like any other.
#[test]
fn the_authority_checks_uploaded_json_against_its_schema_at_any_size() {
    let strict = serde_json::json!({
        "type": "object",
        "properties": { "state": { "enum": ["open", "closed"] } },
        "required": ["state"],
    });
    let server = authority(json_docs(Some(strict)));
    // The writer's replica declares no schema, so it stores anything.
    let writer = replica(json_docs(None), &server, 0x01, alice());
    let large = jazz::large_values::INLINE_VALUE_MAX_BYTES + 1024;
    for (n, len) in [(1, 16), (2, large)] {
        let valid = drive(
            &[&writer, &server],
            writer.insert(
                "docs",
                cells(vec![("payload", json_doc("\"open\"", len))]),
                InsertOptions {
                    row_id: Some(row(1, n)),
                    ..Default::default()
                },
            ),
        )
        .unwrap();
        settle(&writer, &server, valid.mergeable_tx_id()).unwrap();
        let invalid = drive(
            &[&writer, &server],
            writer.insert(
                "docs",
                cells(vec![("payload", json_doc("\"nope\"", len))]),
                InsertOptions {
                    row_id: Some(row(2, n)),
                    ..Default::default()
                },
            ),
        )
        .unwrap();
        let error = settle(&writer, &server, invalid.mergeable_tx_id())
            .expect_err("the authority accepted a value its schema refuses");
        assert!(
            error
                .message
                .contains("JSON schema validation failed for column `payload`"),
            "{len} bytes: {error}"
        );
    }
}
