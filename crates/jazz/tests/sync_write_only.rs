//! Ordinary authenticated write-only acknowledgements are independent of
//! SELECT subscriptions. No initialization journal or E2EE setup is involved.
use super::common;
use jazz::db::{Db, DbConfig, ReadOpts, block_on, duplex};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};
fn author(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}
fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn cells(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("text".into(), Value::String(text.into()))])
}
fn rows(db: &Db) -> Vec<(RowUuid, String)> {
    let q = db.prepare_query(&Query::from("notes")).unwrap();
    let mut rows = block_on(db.all(&q, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            (
                row.row_uuid(),
                match row.cell_at(0).unwrap() {
                    Value::String(s) => s,
                    _ => panic!("text"),
                },
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.0);
    rows
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not settle");
}
fn connect(alice: &Db, bob: &Db, who: &str) {
    let (up, down) = duplex();
    block_on(alice.connect_upstream(up));
    bob.accept_subscriber(down, author(who)).unwrap();
}

// Control uses the ordinary public replica path, without a reservation,
// initialization journal, prepared slot or sealed transaction.
fn ordinary_write_only_case(serializable: bool) {
    use jazz::tools::{PolicyExpr, TablePolicies};
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::False)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::False),
                    ),
            )
            .build(),
    );
    let alice = block_on(Db::open(DbConfig::new(
        schema.clone(),
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([11; 16]),
            author: author("alice"),
        },
    )))
    .unwrap();
    alice.become_replica();
    let bob = block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([13; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    bob.set_authority();
    let first = if serializable {
        let id = OpenTransactionId::new();
        block_on(alice.begin_serializable(id)).unwrap();
        block_on(alice.tx_ref(id).insert(
            "notes",
            cells("founder"),
            jazz::db::InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap();
        block_on(alice.commit_serializable_handle(id)).unwrap().0
    } else {
        block_on(alice.insert(
            "notes",
            cells("founder"),
            jazz::db::InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap()
        .mergeable_tx_id()
    };
    let edit = block_on(alice.update("notes", row(1), cells("rejected edit"), Default::default()))
        .unwrap();
    connect(&alice, &bob, "alice");
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(first, DurabilityTier::Global),
    )
    .unwrap();
    assert!(
        drive(
            &[&alice, &bob],
            alice.wait_for_transaction(edit.mergeable_tx_id(), DurabilityTier::Global)
        )
        .is_err()
    );
    assert_eq!(rows(&alice), vec![(row(1), "founder".into())]);
    assert_eq!(rows(&bob), vec![(row(1), "founder".into())]);
}
/// Alice's ordinary mergeable insert settles; her refused edit must retain
/// the accepted founder on Alice and on Bob's authority.
#[test]
fn ordinary_mergeable_write_only_retains_accepted_founder_after_refused_edit() {
    ordinary_write_only_case(false);
}
/// Alice's ordinary serializable insert follows the same contract without
/// initialization reservation or sealed publication.
#[test]
fn ordinary_serializable_write_only_retains_accepted_founder_after_refused_edit() {
    ordinary_write_only_case(true);
}
