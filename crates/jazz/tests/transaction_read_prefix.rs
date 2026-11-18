//! Ordinary transaction reads retain their lifetime, snapshot and admitted staging.
use crate::common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;
use duplex_transport::duplex;
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, ReadTier, RemoteLinkHint, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::{Pin, pin};
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
    server.accept_subscriber(b, AuthorSubject::SYSTEM).unwrap();
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

fn poll<T>(f: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    f.poll(&mut Context::from_waker(Waker::noop()))
}
fn exchange(alice: &Db, bob: &Db) {
    for _ in 0..4 {
        block_on(alice.tick()).unwrap();
        block_on(bob.tick()).unwrap();
    }
}
fn fixture() -> (Db, Db) {
    let alice = open(1);
    let bob = open(2);
    bob.set_authority();
    connect(&alice, &bob);
    alice.set_remote_link_hint(RemoteLinkHint::Live);
    exchange(&alice, &bob);
    (alice, bob)
}
fn texts(rows: Vec<CurrentRow>) -> Vec<String> {
    rows.into_iter()
        .map(|row| match row.cell("text") {
            Some(Value::String(body)) => body,
            other => panic!("unexpected text: {other:?}"),
        })
        .collect()
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

/// Alice starts a predicate read before committing a staged row while Bob has
/// not answered. A serializable commit must not publish without that read.
/// Alice: stage -> poll read -> commit; Bob: read still unanswered.
#[test]
fn commit_cannot_publish_while_an_admitted_read_is_unfinished() {
    let (alice, bob) = fixture();
    let id = begin(&alice);
    let tx = alice.tx_ref(id);
    block_on(tx.insert("notes", cells("staged"), Default::default())).unwrap();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut read = Box::pin(tx.all_prepared_at(&query, false, None, ReadTier::Remote));
    assert!(poll(read.as_mut()).is_pending());
    let mut committing = Box::pin(alice.commit_serializable_handle(id));
    let result = poll(committing.as_mut());
    if let Poll::Ready(Ok(receipt)) = &result {
        drive(
            &[&alice, &bob],
            alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap();
    }
    let rows = block_on(bob.all(
        &bob.prepare_query(&Query::from("notes")).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap();
    assert!(
        !matches!(result, Poll::Ready(Ok(_))) && rows.is_empty(),
        "unfinished read was bypassed: commit={result:?}, published={:?}",
        texts(rows)
    );
}

/// Alice abandons a transaction with an unfinished read and reuses its public
/// handle ID. Bob's later answer must not attach the old future to the new batch.
#[test]
fn abandoned_read_cannot_attach_to_a_reused_transaction_id() {
    let (alice, bob) = fixture();
    block_on(bob.insert("notes", cells("authority"), Default::default())).unwrap();
    let id = begin(&alice);
    let tx = alice.tx_ref(id);
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut old = Box::pin(tx.all_prepared_at(&query, false, None, ReadTier::Remote));
    assert!(poll(old.as_mut()).is_pending());
    alice.abandon_transaction_handle(id).unwrap();
    block_on(alice.begin_serializable(id)).unwrap();
    let result = drive(&[&alice, &bob], old.as_mut());
    assert!(
        result.is_err(),
        "abandoned read returned rows from a reused transaction: {:?}",
        result.map(texts)
    );
}

/// Alice issues two reads before either future finishes. Bob answers A, then
/// changes B and answers B. Polling A first pins its earlier snapshot; the
/// successful B read must still return B's earlier body, not silently omit it.
/// Alice: poll A -> receive A -> poll B -> receive B -> finish A -> finish B
/// Bob:              answer A -> update B -> answer B
#[test]
fn concurrent_first_reads_do_not_lose_rows_from_the_pinned_snapshot() {
    let (alice, bob) = fixture();
    let a = block_on(bob.insert("notes", cells("a"), Default::default()))
        .unwrap()
        .row_uuid();
    let b = block_on(bob.insert("notes", cells("old-b"), Default::default()))
        .unwrap()
        .row_uuid();
    let id = begin(&alice);
    let tx = alice.tx_ref(id);
    let qa = alice
        .prepare_query(&Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(a.0)))))
        .unwrap();
    let qb = alice
        .prepare_query(&Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(b.0)))))
        .unwrap();
    let mut first = Box::pin(tx.all_prepared_at(&qa, false, None, ReadTier::Remote));
    assert!(poll(first.as_mut()).is_pending());
    exchange(&alice, &bob);
    block_on(bob.update("notes", b, cells("new-b"), Default::default())).unwrap();
    let mut second = Box::pin(tx.all_prepared_at(&qb, false, None, ReadTier::Remote));
    assert!(poll(second.as_mut()).is_pending());
    exchange(&alice, &bob);
    assert_eq!(
        texts(drive(&[&alice, &bob], first.as_mut()).unwrap()),
        ["a"]
    );
    let second = drive(&[&alice, &bob], second.as_mut()).unwrap();
    assert_eq!(
        texts(second),
        ["old-b"],
        "successful read lost a row present at the transaction snapshot"
    );
}

/// Alice drops an admitted ordinary read. Its wake callback can immediately
/// commit the remaining staging: cancellation must neither poison the batch nor
/// invoke callbacks while holding the store or read-state borrow. Bob accepts it.
#[test]
fn cancelling_an_admitted_read_wakes_outside_borrows_and_allows_commit() {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::Wake;
    thread_local! {
        static ON_WAKE: RefCell<Option<Box<dyn Fn()>>> = const { RefCell::new(None) };
    }
    struct CommitOnWake;
    impl Wake for CommitOnWake {
        fn wake(self: Arc<Self>) {
            ON_WAKE.with(|callback| {
                if let Some(callback) = callback.borrow().as_ref() {
                    callback();
                }
            });
        }
    }
    let (alice, bob) = fixture();
    let alice = Rc::new(alice);
    let id = begin(&alice);
    let tx = alice.tx_ref(id);
    block_on(tx.insert("notes", cells("staged"), Default::default())).unwrap();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut read = Box::pin(tx.all_prepared_at(&query, false, None, ReadTier::Remote));
    let waker = Waker::from(Arc::new(CommitOnWake));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let committed = Rc::new(RefCell::new(None));
    ON_WAKE.with(|callback| {
        let alice = alice.clone();
        let committed = committed.clone();
        *callback.borrow_mut() = Some(Box::new(move || {
            *committed.borrow_mut() = Some(block_on(alice.commit_serializable_handle(id)).unwrap());
        }));
    });
    drop(read);
    ON_WAKE.with(|callback| *callback.borrow_mut() = None);
    let receipt = committed
        .borrow_mut()
        .take()
        .expect("cancellation did not wake the read task");
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
    )
    .unwrap();
    let rows = block_on(bob.all(
        &bob.prepare_query(&Query::from("notes")).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap();
    assert_eq!(texts(rows), ["staged"]);
}

/// Alice's pending read moves to another task before abandonment. Its latest
/// task is woken, and its superseded task waker is no longer retained.
#[test]
fn pending_read_replaces_its_task_waker() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::Wake;
    #[derive(Default)]
    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (alice, _bob) = fixture();
    let id = begin(&alice);
    let tx = alice.tx_ref(id);
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut read = Box::pin(tx.all_prepared_at(&query, false, None, ReadTier::Remote));
    let previous = Arc::new(CountWake::default());
    let old = Arc::downgrade(&previous);
    let waker = Waker::from(previous);
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    let current = Arc::new(CountWake::default());
    let waker = Waker::from(current.clone());
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(
        old.upgrade().is_none(),
        "superseded task waker is still retained"
    );
    alice.abandon_transaction_handle(id).unwrap();
    assert_eq!(current.0.load(Ordering::SeqCst), 1);
    assert!(matches!(poll(read.as_mut()), Poll::Ready(Err(_))));
}
