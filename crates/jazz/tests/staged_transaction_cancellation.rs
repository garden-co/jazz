//! Dropping a staged transaction releases its overlay and pending writes.

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

/// Alice stages a write and pauses. The cached overlay and batch are held
/// until she drops the handle; dropping it releases both.
#[test]
fn held_for_test_sees_a_suspended_staged_transaction() {
    let alice = open(1, AuthorSubject::SYSTEM);
    assert!(alice.held_for_test().is_nothing());
    let mut transaction = Box::pin(async {
        let tx = alice.mergeable_tx().await?;
        tx.insert("notes", body("uncommitted"), Default::default())
            .await?;
        pending::<Result<(), Error>>().await
    });
    poll_pending(transaction.as_mut());
    let held = alice.held_for_test();
    assert_eq!(held.open_batches, 1);
    assert!(held.sqlite_transaction);
    drop(transaction);
    assert!(alice.held_for_test().is_nothing());
}

/// Alice cancels a suspended staged transaction after an update and a large insert. Her
/// retained view sees the original row, subscribers hear no rolled-back change,
/// SQLite releases its writer lock, and a subsequent transaction can commit.
///
/// ```text
/// alice: begin -> update + large insert -> await -> drop future -> rollback
/// view:  original row -----------------------------------------> original row
/// ```
#[test]
fn dropping_a_suspended_staged_transaction_releases_rows_chunks_and_the_writer_lock() {
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
    let mut transaction = Box::pin(async {
        let tx = alice.mergeable_tx().await?;
        tx.update("notes", row, body("uncommitted"), Default::default())
            .await?;
        tx.insert("notes", body(&"large".repeat(50_000)), Default::default())
            .await?;
        pending::<Result<(), Error>>().await
    });
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
    block_on(async {
        let tx = view.mergeable_tx().await?;
        tx.update("notes", row, body("committed"), Default::default())
            .await?;
        tx.commit().await
    })
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
    bob.accept_subscriber(server, author).unwrap();
    let mut transaction = Box::pin(async {
        let tx = alice.mergeable_tx().await?;
        tx.insert("notes", body("cancelled"), Default::default())
            .await?;
        pending::<Result<(), Error>>().await
    });
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
    let mut transaction = Box::pin(async {
        let tx = backend.mergeable_tx_for_identity(alice).await?;
        tx.insert("notes", body("cancelled"), Default::default())
            .await?;
        pending::<Result<(), Error>>().await
    });
    poll_pending(transaction.as_mut());
    drop(transaction);
    assert!(bodies(&backend).is_empty());
    for (author, label) in [(bob, "bob"), (alice, "alice")] {
        block_on(async {
            let tx = backend.mergeable_tx_for_identity(author).await?;
            tx.insert("notes", body(label), Default::default()).await?;
            tx.commit().await
        })
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
    let mut transaction = Box::pin(async {
        let tx = alice.mergeable_tx().await?;
        tx.insert("notes", body("gone"), Default::default()).await?;
        // Existing fault injection reproduces SQLite's whole-transaction
        // rollback after disk-full/I/O errors; no public API causes that fault.
        alice.rollback_sqlite_transaction_for_test();
        pending::<Result<(), Error>>().await
    });
    poll_pending(transaction.as_mut());
    drop(transaction);
    assert!(bodies(&alice).is_empty());
    block_on(async {
        let tx = alice.mergeable_tx().await?;
        tx.insert("notes", body("recovered"), Default::default())
            .await?;
        tx.commit().await
    })
    .unwrap();
    assert_eq!(bodies(&alice), [Value::String("recovered".to_owned())]);
}

/// Alice catches a panic while holding a staged transaction. Unwinding must release
/// the same transaction state as cancellation, leaving no row and allowing retry.
#[test]
fn unwinding_a_staged_transaction_rolls_back_before_the_owner_is_reused() {
    let alice = open(1, AuthorSubject::SYSTEM);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<(), Error> = block_on(async {
            let tx = alice.mergeable_tx().await?;
            tx.insert("notes", body("panicked"), Default::default())
                .await?;
            panic!("application staged transaction panic");
        });
    }));
    assert!(panic.is_err());
    assert!(bodies(&alice).is_empty());
    block_on(async {
        let tx = alice.mergeable_tx().await?;
        tx.insert("notes", body("retry"), Default::default())
            .await?;
        tx.commit().await
    })
    .unwrap();
    assert_eq!(bodies(&alice), [Value::String("retry".to_owned())]);
}

/// While Alice's staged transaction is suspended, her store learns it must never hand
/// out a seq at or below a floor (a peer's position, a host's seeded floor).
/// Rolling her unit back, by cancellation or by error, undoes only the unit:
/// the floor stays, so her next commit's transaction id is above it and the
/// authority cannot mistake it for one it already deduplicated.
#[test]
fn rolling_back_a_staged_transaction_keeps_seq_floors_raised_while_it_ran() {
    for cancel in [true, false] {
        let alice = open(1, AuthorSubject::for_test_bytes([1; 16]));
        let floor = alice.seq_high_water().saturating_mul(2) + 1_000;
        let mut transaction = Box::pin(async {
            let tx = alice.mergeable_tx().await?;
            tx.insert("notes", body("rolled back"), Default::default())
                .await?;
            alice.seed_seq_floor(floor);
            if cancel {
                pending::<Result<(), Error>>().await
            } else {
                Err(Error::new(jazz::db::ErrorCode::Protocol, "refused"))
            }
        });
        if cancel {
            poll_pending(transaction.as_mut());
            drop(transaction);
        } else {
            assert!(block_on(transaction).is_err());
        }
        assert!(bodies(&alice).is_empty());
        assert!(alice.seq_high_water() >= floor, "cancel: {cancel}");
        let ((), receipt) = block_on(async {
            let tx = alice.mergeable_tx().await?;
            tx.insert("notes", body("committed"), Default::default())
                .await?;
            let receipt = tx.commit().await?;
            Ok::<_, jazz::db::Error>(((), receipt))
        })
        .unwrap();
        assert!(receipt.0 > floor, "cancel: {cancel}");
    }
}

/// Alice pauses a staged transaction. Carol's standalone write commits
/// independently; Alice sees it only after reading outside her own staging.
#[test]
fn a_write_as_another_identity_commits_independently() {
    use jazz::db::{InsertOptions, WriteIdentity};
    let alice = open(1, AuthorSubject::SYSTEM);
    let carol = AuthorSubject::for_test_bytes([7; 16]);
    let tx = block_on(alice.mergeable_tx()).unwrap();
    block_on(tx.insert("notes", body("own"), Default::default())).unwrap();
    block_on(alice.insert(
        "notes",
        body("carol's"),
        InsertOptions {
            identity: WriteIdentity::Session(carol),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(bodies(&alice), [Value::String("carol's".to_owned())]);
    block_on(tx.commit()).unwrap();
    let mut visible = bodies(&alice);
    visible.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(
        visible,
        [
            Value::String("carol's".to_owned()),
            Value::String("own".to_owned())
        ]
    );
}

/// Alice commits an ID-based handle while a second staged transaction is
/// suspended. The first commit survives whether the second commits or is
/// abandoned. With syncing enabled, Bob's authority sees those same commits.
fn handle_commits_beside_a_suspended_transaction(resume: bool, synced: bool) {
    use jazz::model::transaction::OpenTransactionId;
    let author = AuthorSubject::for_test_bytes([1; 16]);
    let alice = open(
        1,
        if synced {
            author
        } else {
            AuthorSubject::SYSTEM
        },
    );
    let bob = open(2, AuthorSubject::SYSTEM);
    let _upstream = synced.then(|| {
        let (client, server) = duplex();
        let upstream = block_on(alice.connect_upstream(client));
        bob.accept_subscriber(server, author).unwrap();
        upstream
    });
    let handle = OpenTransactionId::new();
    block_on(alice.begin_mergeable(handle)).unwrap();
    block_on(
        alice
            .tx_ref(handle)
            .insert("notes", body("handle's"), Default::default()),
    )
    .unwrap();
    let (finish, finished) = futures_channel::oneshot::channel::<()>();
    let mut transaction = Box::pin(async {
        let tx = alice.mergeable_tx().await?;
        tx.insert("notes", body("staged transaction's"), Default::default())
            .await?;
        finished.await.unwrap();
        let receipt = tx.commit().await?;
        Ok::<_, jazz::db::Error>(((), receipt))
    });
    poll_pending(transaction.as_mut());

    let committed = block_on(alice.commit_mergeable_handle(handle)).unwrap();
    let mut last_commit = committed.0;
    assert_eq!(bodies(&alice), [Value::String("handle's".to_owned())]);

    let mut expected = vec![Value::String("handle's".to_owned())];
    if resume {
        finish.send(()).unwrap();
        last_commit = block_on(transaction).unwrap().1.0;
        expected.push(Value::String("staged transaction's".to_owned()));
    } else {
        drop(transaction);
    }
    expected.sort_by_key(|value| format!("{value:?}"));
    let sorted = |db: &Db| {
        let mut bodies = bodies(db);
        bodies.sort_by_key(|value| format!("{value:?}"));
        bodies
    };
    assert_eq!(sorted(&alice), expected);
    if synced {
        drive(
            &alice,
            &bob,
            alice.wait_for_transaction(last_commit, DurabilityTier::Global),
        )
        .unwrap();
        assert_eq!(sorted(&bob), expected);
        assert_eq!(sorted(&alice), expected);
    }
}

/// Alice commits one transaction while another is paused, then commits both.
#[test]
fn a_handle_commits_beside_a_suspended_transaction_that_resumes() {
    handle_commits_beside_a_suspended_transaction(true, false);
}

/// Alice abandoning the paused transaction cannot undo her independent commit.
#[test]
fn a_handle_commits_beside_a_suspended_transaction_that_is_cancelled() {
    handle_commits_beside_a_suspended_transaction(false, false);
}

/// Bob's authority receives both of Alice's independently committed transactions.
#[test]
fn a_client_handle_commits_beside_a_suspended_transaction_that_resumes() {
    handle_commits_beside_a_suspended_transaction(true, true);
}

/// Bob receives Alice's committed write, but never her abandoned staged write.
#[test]
fn a_client_handle_commits_beside_a_suspended_transaction_that_is_cancelled() {
    handle_commits_beside_a_suspended_transaction(false, true);
}

/// Alice abandons her staged write after Bob commits through the same Db.
/// alice: stage -> suspend -> drop
/// bob:              insert -> survives
#[test]
fn abandoning_a_transaction_keeps_an_unrelated_write() {
    let db = open(1, AuthorSubject::SYSTEM);
    let tx = block_on(db.mergeable_tx()).unwrap();
    block_on(tx.insert("notes", body("inside"), Default::default())).unwrap();
    block_on(db.insert("notes", body("outside"), Default::default())).unwrap();
    drop(tx);
    assert_eq!(bodies(&db), [Value::String("outside".to_owned())]);
}
