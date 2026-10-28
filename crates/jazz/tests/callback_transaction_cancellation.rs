//! Cancelling a callback transaction releases its SQLite and runtime state.

use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, Error, FileOptions, ReadOpts, ReadTier, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::collections::BTreeMap;
use std::future::{Future, pending};
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};

fn schema() -> JazzSchema {
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

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn open(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(config(node, author))).unwrap()
}

fn body(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".to_owned(), Value::String(text.to_owned()))])
}

fn read(db: &Db, query: &Query) -> Vec<CurrentRow> {
    let prepared = db.prepare_query(query).unwrap();
    block_on(db.all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalOnly))).unwrap()
}

fn bodies(db: &Db) -> Vec<Value> {
    read(db, &Query::from("notes"))
        .iter()
        .map(|row| row.cell("body").unwrap())
        .collect()
}

fn poll_pending<T>(future: Pin<&mut impl Future<Output = T>>) {
    assert!(
        future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

fn drive<T>(alice: &Db, bob: &Db, future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..100 {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        block_on(alice.tick()).unwrap();
        block_on(bob.tick()).unwrap();
    }
    panic!("sync operation did not finish within 100 turns");
}

/// Alice cancels a suspended callback after an update and a large insert. Her
/// retained view sees the original row, subscribers hear no rolled-back change,
/// SQLite releases its writer lock, and a subsequent transaction can commit.
///
/// ```text
/// alice: begin -> update + large insert -> await -> drop future -> rollback
/// view:  original row -----------------------------------------> original row
/// ```
#[test]
fn dropping_a_suspended_callback_releases_rows_chunks_and_the_writer_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    let alice = block_on(Db::open_at(
        config(1, AuthorSubject::SYSTEM),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    let view = block_on(alice.register_schema_view(schema())).unwrap();
    let row = block_on(alice.insert("notes", body("original"), Default::default()))
        .unwrap()
        .row_uuid();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut subscription = block_on(alice.subscribe(&query, ReadOpts::default())).unwrap();
    assert!(subscription.try_next_event().is_some());
    let mut transaction = Box::pin(alice.transaction(async |tx| {
        tx.update("notes", row, body("uncommitted"), Default::default())
            .await?;
        tx.insert("notes", body(&"large".repeat(50_000)), Default::default())
            .await?;
        pending::<Result<(), Error>>().await
    }));
    poll_pending(transaction.as_mut());
    drop(transaction);
    let visible = bodies(&view);
    assert_eq!(visible.len(), 1, "cancelled insert must be rolled back");
    assert_eq!(visible[0], Value::String("original".to_owned()));
    block_on(alice.tick()).unwrap();
    assert!(subscription.try_next_event().is_none());
    // Cancelled staging bytes have no visible row, so use the existing node
    // inventory to check reclamation in addition to logical reads.
    assert_eq!(alice.large_value_nodes_for_test().unwrap(), (0, 0));
    // A second connection tests the physical writer-lock boundary, which the
    // public database API deliberately does not expose.
    let contender = rusqlite::Connection::open(&path).unwrap();
    contender
        .execute_batch("BEGIN IMMEDIATE; ROLLBACK")
        .unwrap();
    block_on(view.transaction(async |tx| {
        tx.update("notes", row, body("committed"), Default::default())
            .await?;
        Ok(())
    }))
    .unwrap();
    drop(subscription);
    drop(view);
    drop(alice);
    let reopened = block_on(Db::open_at(
        config(1, AuthorSubject::SYSTEM),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    assert_eq!(bodies(&reopened), [Value::String("committed".to_owned())]);
}

/// Alice's cancelled local transaction cannot hitchhike on her next upload.
/// Bob's authority receives only the later committed row and settles it normally.
///
/// ```text
/// alice: cancelled insert -> drop -> later commit -> upload -> bob -> receipt
/// bob:   no cancelled row ----------------------------------> later row only
/// ```
#[test]
fn cancelled_writes_do_not_leak_into_the_next_authority_upload() {
    let author = AuthorSubject::for_test_bytes([1; 16]);
    let alice = open(1, author);
    let bob = open(2, AuthorSubject::SYSTEM);
    let (client, server) = duplex();
    let _upstream = block_on(alice.connect_upstream(client));
    bob.accept_subscriber(server, author);
    let mut transaction = Box::pin(alice.transaction(async |tx| {
        tx.insert("notes", body("cancelled"), Default::default())
            .await?;
        pending::<Result<(), Error>>().await
    }));
    poll_pending(transaction.as_mut());
    drop(transaction);
    assert!(bodies(&alice).is_empty());
    let write = block_on(alice.insert("notes", body("committed"), Default::default())).unwrap();
    drive(
        &alice,
        &bob,
        alice.wait_for_transaction(write.mergeable_tx_id(), DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(bodies(&alice), [Value::String("committed".to_owned())]);
    assert_eq!(bodies(&bob), [Value::String("committed".to_owned())]);
}

/// Alice's cancelled write allocated an author code that SQLite rolls back.
/// Bob may reuse that code; later Alice writes must still carry Alice's identity.
///
/// ```text
/// backend: alice(cancel) -> bob(commit) -> alice(commit) -> distinct provenance
/// ```
#[test]
fn cancelled_author_codes_cannot_alias_a_later_writer() {
    let backend = open(1, AuthorSubject::SYSTEM);
    let alice = AuthorSubject::for_test_bytes([1; 16]);
    let bob = AuthorSubject::for_test_bytes([2; 16]);
    let mut transaction = Box::pin(backend.transaction_for_identity(alice, async |tx| {
        tx.insert("notes", body("cancelled"), Default::default())
            .await?;
        pending::<Result<(), Error>>().await
    }));
    poll_pending(transaction.as_mut());
    drop(transaction);
    assert!(bodies(&backend).is_empty());
    for (author, label) in [(bob, "bob"), (alice, "alice")] {
        block_on(backend.transaction_for_identity(author, async |tx| {
            tx.insert("notes", body(label), Default::default()).await?;
            Ok(())
        }))
        .unwrap();
    }
    let rows = read(
        &backend,
        &Query::from("notes").select(["body", "$createdBy.identity.subject"]),
    );
    assert_eq!(rows.len(), 2);
    for (author, label) in [(alice, "alice"), (bob, "bob")] {
        let row = rows
            .iter()
            .find(|row| row.cell("body") == Some(Value::String(label.to_owned())))
            .unwrap();
        assert_eq!(
            row.cell("$createdBy.identity.subject"),
            Some(Value::Nullable(Some(Box::new(Value::String(
                author.principal_parts().1,
            )))))
        );
    }
}

/// SQLite may already have rolled Alice's transaction back after an I/O error.
/// Cancelling its future must tolerate the missing savepoint and keep the owner
/// usable for the next write.
#[test]
fn cancellation_after_sqlite_automatic_rollback_keeps_the_owner_usable() {
    let alice = open(1, AuthorSubject::SYSTEM);
    let mut transaction = Box::pin(alice.transaction(async |tx| {
        tx.insert("notes", body("gone"), Default::default()).await?;
        // Existing fault injection reproduces SQLite's whole-transaction
        // rollback after disk-full/I/O errors; no public API causes that fault.
        alice.rollback_sqlite_transaction_for_test();
        pending::<Result<(), Error>>().await
    }));
    poll_pending(transaction.as_mut());
    drop(transaction);
    assert!(bodies(&alice).is_empty());
    block_on(alice.transaction(async |tx| {
        tx.insert("notes", body("recovered"), Default::default())
            .await?;
        Ok(())
    }))
    .unwrap();
    assert_eq!(bodies(&alice), [Value::String("recovered".to_owned())]);
}

/// Alice catches a panic from her transaction callback. Unwinding must release
/// the same transaction state as cancellation, leaving no row and allowing retry.
#[test]
fn unwinding_a_callback_rolls_back_before_the_owner_is_reused() {
    let alice = open(1, AuthorSubject::SYSTEM);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<((), _), Error> = block_on(alice.transaction(async |tx| {
            tx.insert("notes", body("panicked"), Default::default())
                .await?;
            panic!("application callback panic");
        }));
    }));
    assert!(panic.is_err());
    assert!(bodies(&alice).is_empty());
    block_on(alice.transaction(async |tx| {
        tx.insert("notes", body("retry"), Default::default())
            .await?;
        Ok(())
    }))
    .unwrap();
    assert_eq!(bodies(&alice), [Value::String("retry".to_owned())]);
}
