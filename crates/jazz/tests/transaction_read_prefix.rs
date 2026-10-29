//! Ordinary transaction reads retain the writes staged at their admission.
use crate::common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;
use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

fn open(node: u8) -> Db {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}
fn connect(client: &Db, server: &Db) {
    let (a, b) = duplex();
    block_on(client.connect_upstream(a));
    server.accept_subscriber(b, AuthorSubject::SYSTEM);
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
    panic!("operation did not finish")
}
fn cells(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("text".into(), Value::String(text.into()))])
}
fn begin(db: &Db) -> OpenTransactionId {
    let id = OpenTransactionId::new();
    block_on(db.begin_serializable(id)).unwrap();
    id
}

/// Alice admits ordinary reads before and after a staged update, while Bob's
/// authority has not answered either read. Each read sees its own prefix, even
/// for chunked text; commit still publishes all later writes.
///
/// Alice: first -> read A -> middle -> read B -> last
/// Bob:   reply A -> reply B -> accept last
#[test]
fn ordinary_reads_preserve_admitted_staging_for_inline_and_chunked_text() {
    for large in [false, true] {
        let server = open(3);
        let alice = open(1);
        connect(&alice, &server);
        let tx = begin(&alice);
        let transaction = alice.tx_ref(tx);
        let value = |label: &str| {
            if large {
                format!("{}-{label}", "chunked-é😀".repeat(10_000))
            } else {
                label.to_owned()
            }
        };
        let first = value("first");
        let middle = value("middle");
        let last = value("last");
        let row = block_on(transaction.insert("notes", cells(&first), Default::default()))
            .unwrap()
            .row_uuid();
        let query = alice.prepare_query(&Query::from("notes")).unwrap();
        let mut a = Box::pin(transaction.all_prepared(&query));
        assert!(
            a.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        block_on(transaction.update("notes", row, cells(&middle), Default::default())).unwrap();
        let mut b = Box::pin(transaction.all_prepared(&query));
        assert!(
            b.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        block_on(transaction.update("notes", row, cells(&last), Default::default())).unwrap();
        for (read, expected) in [(&mut a, &first), (&mut b, &middle)] {
            let rows = drive(&[&alice, &server], read.as_mut()).unwrap();
            assert_eq!(rows.len(), 1);
            let text = rows[0]
                .cells()
                .find(|(name, _)| *name == "text")
                .unwrap()
                .1
                .unwrap();
            assert_eq!(text, &Value::String(expected.clone()));
        }
        let receipt = block_on(alice.commit_serializable_handle(tx)).unwrap();
        drive(
            &[&alice, &server],
            alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap();
        let rows = block_on(server.all(
            &server.prepare_query(&Query::from("notes")).unwrap(),
            ReadOpts::default(),
        ))
        .unwrap();
        assert_eq!(rows.len(), 1);
        let text = rows[0]
            .cells()
            .find(|(name, _)| *name == "text")
            .unwrap()
            .1
            .unwrap();
        assert_eq!(text, &Value::String(last));
    }
}

/// Alice cancels the active read while a second is queued. The surviving read
/// keeps the rows staged at its own admission, despite a later delete/update;
/// cancellation releases its predecessor without poisoning Global commit.
#[test]
fn ordinary_queued_read_keeps_its_prefix_after_predecessor_cancellation() {
    let server = open(3);
    let alice = open(1);
    connect(&alice, &server);
    let tx = begin(&alice);
    let transaction = alice.tx_ref(tx);
    let first = block_on(transaction.insert("notes", cells("first"), Default::default()))
        .unwrap()
        .row_uuid();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut a = Box::pin(transaction.all_prepared(&query));
    assert!(
        a.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let second = block_on(transaction.insert("notes", cells("middle"), Default::default()))
        .unwrap()
        .row_uuid();
    let mut b = Box::pin(transaction.all_prepared(&query));
    assert!(
        b.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    block_on(transaction.delete("notes", first, Default::default())).unwrap();
    block_on(transaction.update("notes", second, cells("last"), Default::default())).unwrap();
    drop(a);
    let rows = drive(&[&alice, &server], b.as_mut()).unwrap();
    let mut actual = rows
        .iter()
        .map(|row| {
            row.cells()
                .find(|(name, _)| *name == "text")
                .unwrap()
                .1
                .unwrap()
                .clone()
        })
        .collect::<Vec<_>>();
    actual.sort_by_key(|value| match value {
        Value::String(text) => text.clone(),
        other => panic!("unexpected text: {other:?}"),
    });
    assert_eq!(
        actual,
        [
            Value::String("first".into()),
            Value::String("middle".into())
        ]
    );
    let receipt = block_on(alice.commit_serializable_handle(tx)).unwrap();
    drive(
        &[&alice, &server],
        alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
    )
    .unwrap();
    let rows = block_on(server.all(
        &server.prepare_query(&Query::from("notes")).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), second);
    assert_eq!(
        rows[0]
            .cells()
            .find(|(name, _)| *name == "text")
            .unwrap()
            .1
            .unwrap(),
        &Value::String("last".into())
    );
}
