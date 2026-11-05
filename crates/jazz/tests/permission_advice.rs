//! Permission advice: a replica asks its upstream authority whether an
//! operation would be allowed for its session. The authority dry-runs the
//! operation under the session's policies and rolls it back; without an
//! authority the answer is `Unknown`, never a local guess.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{PermissionAdvice, PermissionAdviceAction};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};

use common::compile_schema;

fn owned_by_session() -> PolicyExpr {
    PolicyExpr::Cmp {
        column: "owner".to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec![
            "user".to_owned(),
            "identity".to_owned(),
            "subject".to_owned(),
        ]),
    }
}

fn schema() -> JazzSchema {
    let owned = owned_by_session();
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(owned.clone())
                            .with_insert(owned.clone())
                            .with_update(Some(owned.clone()), owned.clone())
                            .with_delete(owned),
                    ),
            )
            .table(
                TableSchemaBuilder::new("pinned")
                    .column("title", ColumnType::Text)
                    .policies(TablePolicies::new().with_insert(PolicyExpr::Exists {
                        table: "notes".to_owned(),
                        condition: Box::new(owned_by_session()),
                    })),
            )
            .build(),
    )
}

fn open(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

fn subject(author: AuthorSubject) -> Value {
    Value::String(author.principal_parts().1)
}

fn note(owner: AuthorSubject, title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("owner".to_owned(), subject(owner)),
        ("title".to_owned(), Value::String(title.to_owned())),
    ])
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..100 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn ask(client: &Db, server: &Db, action: PermissionAdviceAction) -> PermissionAdvice {
    let id = client.request_permission_advice(action, None);
    drive(&[client, server, client], client.permission_advice(id))
}

fn connect(client: &Db, server: &Db, author: AuthorSubject) {
    let (client_side, server_side) = duplex();
    block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, author);
}

fn count(db: &Db, table: &str) -> usize {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .len()
}

#[test]
fn the_authority_advises_each_action_under_the_sessions_policies() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let row = RowUuid::from_bytes([0x11; 16]);
    block_on(server.insert(
        "notes",
        note(alice, "mine"),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
    let alice_db = open(0xa1, alice);
    let bob_db = open(0xb0, bob);
    connect(&alice_db, &server, alice);
    connect(&bob_db, &server, bob);

    use PermissionAdvice::{Allowed, Denied};
    let insert = |owner| PermissionAdviceAction::Insert {
        table: "notes".to_owned(),
        cells: note(owner, "new"),
    };
    let read = || PermissionAdviceAction::Read {
        table: "notes".to_owned(),
        row,
    };
    let update = || PermissionAdviceAction::Update {
        table: "notes".to_owned(),
        row,
        patch: BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]),
    };
    let delete = || PermissionAdviceAction::Delete {
        table: "notes".to_owned(),
        row,
    };
    assert_eq!(ask(&alice_db, &server, insert(alice)), Allowed);
    assert_eq!(ask(&alice_db, &server, insert(bob)), Denied);
    assert_eq!(ask(&alice_db, &server, read()), Allowed);
    assert_eq!(ask(&alice_db, &server, update()), Allowed);
    assert_eq!(ask(&alice_db, &server, delete()), Allowed);
    assert_eq!(ask(&bob_db, &server, read()), Denied);
    assert_eq!(ask(&bob_db, &server, update()), Denied);
    assert_eq!(ask(&bob_db, &server, delete()), Denied);
    // An insert policy that reads other rows is decided for a fresh row:
    // alice owns a note, bob none.
    let pinned = PermissionAdviceAction::Insert {
        table: "pinned".to_owned(),
        cells: BTreeMap::from([("title".to_owned(), Value::String("p".to_owned()))]),
    };
    assert_eq!(ask(&alice_db, &server, pinned.clone()), Allowed);
    assert_eq!(ask(&bob_db, &server, pinned), Denied);

    // Advice never writes.
    assert_eq!(count(&server, "notes"), 1);
    let prepared = server.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(server.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(
        rows[0]
            .cells()
            .find(|(name, _)| *name == "title")
            .unwrap()
            .1,
        Some(&Value::String("mine".to_owned()))
    );
}

#[test]
fn without_an_authority_or_once_cancelled_the_advice_is_unknown() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let action = || PermissionAdviceAction::Insert {
        table: "notes".to_owned(),
        cells: note(alice, "new"),
    };
    // Never connected: no authority to ask, and no local guess.
    let alone = open(0xa1, alice);
    let id = alone.request_permission_advice(action(), None);
    assert_eq!(
        block_on(alone.permission_advice(id)),
        PermissionAdvice::Unknown
    );

    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let client = open(0xa2, alice);
    connect(&client, &server, alice);
    let id = client.request_permission_advice(action(), None);
    client.cancel_permission_advice_request(id);
    // The late answer is dropped.
    assert_eq!(
        drive(&[&client, &server, &client], client.permission_advice(id)),
        PermissionAdvice::Unknown
    );
}

/// Alice asks an authority to edit her and Bob's rows. Optional UPDATE clauses
/// constrain only their named images; no UPDATE clauses still denies the edit.
/// Advice must agree with the real uploaded write and must not change rows.
#[test]
fn optional_update_clauses_agree_for_advice_and_uploaded_writes() {
    use jazz::db::UpdateOptions;
    use jazz::model::public_schema::OperationPolicy;
    use jazz::tx::DurabilityTier;
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let owned = owned_by_session();
    let cases = [
        (OperationPolicy::using(owned.clone()), alice, bob, true),
        (OperationPolicy::using(owned.clone()), bob, alice, false),
        (OperationPolicy::with_check(owned.clone()), bob, alice, true),
        (
            OperationPolicy::with_check(owned.clone()),
            alice,
            bob,
            false,
        ),
        (
            OperationPolicy::using_and_check(owned.clone(), owned.clone()),
            alice,
            alice,
            true,
        ),
        (
            OperationPolicy::using_and_check(owned.clone(), owned.clone()),
            alice,
            bob,
            false,
        ),
        (
            OperationPolicy::using_and_check(owned.clone(), owned),
            bob,
            alice,
            false,
        ),
        (OperationPolicy::default(), alice, alice, false),
    ];
    for (update, before, after, allowed) in cases {
        let schema = compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("owner", ColumnType::Text)
                        .column("title", ColumnType::Text)
                        .policies(TablePolicies {
                            update,
                            ..TablePolicies::new().with_select(PolicyExpr::True)
                        }),
                )
                .build(),
        );
        let server = block_on(Db::open_history_complete(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0xee; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        let client = block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0xa1; 16]),
                author: alice,
            },
        )))
        .unwrap();
        let row = block_on(server.insert("notes", note(before, "original"), Default::default()))
            .unwrap()
            .row_uuid();
        connect(&client, &server, alice);
        let query = client.prepare_query(&Query::from("notes")).unwrap();
        let _subscription = block_on(client.subscribe(&query, ReadOpts::default())).unwrap();
        for _ in 0..4 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        assert_eq!(
            count(&client, "notes"),
            1,
            "Alice has the current row before editing"
        );
        let patch = note(after, "edited");
        let advice = ask(
            &client,
            &server,
            PermissionAdviceAction::Update {
                table: "notes".into(),
                row,
                patch: patch.clone(),
            },
        );
        assert_eq!(
            advice,
            if allowed {
                PermissionAdvice::Allowed
            } else {
                PermissionAdvice::Denied
            }
        );
        let query = server.prepare_query(&Query::from("notes")).unwrap();
        let read = || block_on(server.all(&query, ReadOpts::default())).unwrap();
        assert_eq!(
            read()[0]
                .cells()
                .find(|(name, _)| *name == "title")
                .unwrap()
                .1,
            Some(&Value::String("original".into())),
            "advice never mutates the row"
        );
        let write = block_on(client.update("notes", row, patch, UpdateOptions::default())).unwrap();
        let result = drive(
            &[&client, &server, &client],
            client.wait_for_transaction(write.mergeable_tx_id(), DurabilityTier::Global),
        );
        assert_eq!(
            result.is_ok(),
            allowed,
            "advice and real write must agree: {result:?}"
        );
        assert_eq!(
            read()[0]
                .cells()
                .find(|(name, _)| *name == "title")
                .unwrap()
                .1,
            Some(&Value::String(
                if allowed { "edited" } else { "original" }.into()
            ))
        );
    }
}

#[test]
fn a_trusted_backend_asks_for_the_sessions_it_acts_for() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let server = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let row = RowUuid::from_bytes([0x11; 16]);
    block_on(server.insert(
        "notes",
        note(alice, "mine"),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
    let backend = open(0xbe, AuthorSubject::SYSTEM);
    connect(&backend, &server, AuthorSubject::SYSTEM);
    let alice_db = open(0xa1, alice);
    connect(&alice_db, &server, alice);
    let update = || PermissionAdviceAction::Update {
        table: "notes".to_owned(),
        row,
        patch: BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]),
    };
    let ask_as = |client: &Db, author: AuthorSubject| {
        let id = client.request_permission_advice(update(), Some((author, BTreeMap::new())));
        drive(&[client, &server, client], client.permission_advice(id))
    };
    assert_eq!(ask_as(&backend, alice), PermissionAdvice::Allowed);
    assert_eq!(ask_as(&backend, bob), PermissionAdvice::Denied);
    // A session link cannot ask as another session: it is answered as
    // itself.
    assert_eq!(ask_as(&alice_db, bob), PermissionAdvice::Allowed);
}
