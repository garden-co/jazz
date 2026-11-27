//! Ordinary transaction reads retain their lifetime, snapshot and admitted staging.
use crate::common;
use jazz_types::legacy_test_future::ResultFutureExt;
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
    jazz::db::block_on(bob.set_authority()).unwrap();
    connect(&alice, &bob);
    jazz::db::block_on(alice.set_remote_link_hint(RemoteLinkHint::Live));
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

#[cfg(feature = "e2ee-client")]
mod ledger {
    use super::*;
    use jazz::db::{ErrorCode, ReadSpan, SyncMessage, Transport, Upstream};

    // Hold selected fetches and reorder authority replies through the public
    // transport so the tests can distinguish issued reads from completed ones.
    struct Link {
        client: Box<dyn Transport>,
        server: Box<dyn Transport>,
        _upstream: Upstream,
    }

    impl Link {
        fn new(alice: &Db, bob: &Db) -> Self {
            let (client_db, client) = duplex();
            let (server_db, server) = duplex();
            let upstream = block_on(alice.connect_upstream(client_db));
            bob.accept_subscriber(server_db, AuthorSubject::SYSTEM)
                .unwrap();
            block_on(alice.set_remote_link_hint(RemoteLinkHint::Live));
            let mut link = Self {
                client,
                server,
                _upstream: upstream,
            };
            for _ in 0..4 {
                block_on(alice.tick()).unwrap();
                while let Some(message) = link.client.try_recv() {
                    link.server.send(message).unwrap();
                }
                link.reply(bob, []);
            }
            block_on(alice.tick()).unwrap();
            link
        }

        fn requests(&mut self, alice: &Db) -> Vec<SyncMessage> {
            block_on(alice.tick()).unwrap();
            let mut reads = Vec::new();
            while let Some(message) = self.client.try_recv() {
                if matches!(message, SyncMessage::Read { .. }) {
                    reads.push(message);
                } else {
                    self.server.send(message).unwrap();
                }
            }
            reads
        }

        fn reply(&mut self, bob: &Db, requests: impl IntoIterator<Item = SyncMessage>) {
            for message in requests {
                self.server.send(message).unwrap();
            }
            block_on(bob.tick()).unwrap();
            while let Some(message) = self.server.try_recv() {
                self.client.send(message).unwrap();
            }
        }

        fn pin(&mut self, alice: &Db, bob: &Db, id: OpenTransactionId) -> u64 {
            let tx = alice.tx_ref(id);
            let query = alice.prepare_query(&Query::from("notes")).unwrap();
            let mut read = pin!(tx.ledger_read(&query));
            assert!(poll(read.as_mut()).is_pending());
            let requests = self.requests(alice);
            assert_eq!(requests.len(), 1);
            self.reply(bob, requests);
            block_on(alice.tick()).unwrap();
            match poll(read.as_mut()) {
                Poll::Ready(result) => result.unwrap().through,
                Poll::Pending => panic!("the authority already answered the first read"),
            }
        }

        fn refuse(&mut self, request: SyncMessage, message: &str) {
            let SyncMessage::Read { id, .. } = request else {
                panic!("expected a read request");
            };
            self.client
                .send(SyncMessage::Refused {
                    id,
                    code: ErrorCode::Query,
                    message: message.to_owned(),
                })
                .unwrap();
        }
    }

    fn fixture() -> (Db, Db, Link) {
        let alice = open(1);
        let bob = open(2);
        block_on(bob.set_authority()).unwrap();
        let link = Link::new(&alice, &bob);
        (alice, bob, link)
    }

    /// Bob answers Alice's second fetch first. Alice returns both results
    /// and sends their read records to Bob in the order she admitted them.
    /// Alice: pin -> admit A -> admit B -> return A -> return B -> commit
    /// Bob:                     reply B -> reply A
    #[test]
    fn successful_overlapping_ledger_reads_record_and_return_in_admission_order() {
        let (alice, bob, mut link) = fixture();
        let first_row = block_on(bob.insert("notes", cells("first"), Default::default()))
            .unwrap()
            .row_uuid();
        let second_row = block_on(bob.insert("notes", cells("second"), Default::default()))
            .unwrap()
            .row_uuid();
        let id = begin(&alice);
        let tx = alice.tx_ref(id);
        let cut = link.pin(&alice, &bob, id);
        let first_query = alice
            .prepare_query(
                &Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(first_row.0)))),
            )
            .unwrap();
        let second_query = alice
            .prepare_query(
                &Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(second_row.0)))),
            )
            .unwrap();
        let mut first = pin!(tx.ledger_read(&first_query));
        let mut second = pin!(tx.ledger_read(&second_query));
        assert!(poll(first.as_mut()).is_pending());
        assert!(poll(second.as_mut()).is_pending());
        let mut requests = link.requests(&alice);
        assert_eq!(requests.len(), 2);
        link.reply(&bob, [requests.pop().unwrap()]);
        block_on(alice.tick()).unwrap();
        assert!(poll(first.as_mut()).is_pending());
        assert!(
            poll(second.as_mut()).is_pending(),
            "the second reply arrived before the first"
        );

        link.reply(&bob, requests);
        block_on(alice.tick()).unwrap();
        for (read, expected) in [(&mut first, first_row), (&mut second, second_row)] {
            let Poll::Ready(Ok(read)) = poll(read.as_mut()) else {
                panic!("the authority has answered both reads");
            };
            assert_eq!(read.through, cut);
            assert_eq!(read.rows.len(), 1);
            assert_eq!(read.rows[0].0.row_uuid(), expected);
        }

        // The outgoing commit exposes the order of the recorded reads.
        block_on(tx.insert("notes", cells("staged"), Default::default())).unwrap();
        block_on(alice.commit_serializable_handle(id)).unwrap();
        block_on(alice.tick()).unwrap();
        let reads = std::iter::from_fn(|| link.client.try_recv())
            .find_map(|message| match message {
                SyncMessage::Commit {
                    kind: jazz::db::WireKind::Serializable { reads, .. },
                    ..
                } => Some(reads),
                _ => None,
            })
            .expect("the transaction sends a serializable commit");
        assert_eq!(reads.len(), 3);
        assert_eq!(reads[1].rows.len(), 1);
        assert_eq!(reads[2].rows.len(), 1);
        assert_eq!(reads[1].rows[0].0, *first_row.0.as_bytes());
        assert_eq!(reads[2].rows[0].0, *second_row.0.as_bytes());
    }

    /// Alice admits B while A selects a snapshot from Bob. Only after A
    /// succeeds may B send a fetch, and it must ask at A's cut.
    /// Alice: admit A -> admit B -> pin A -> fetch B
    /// Bob:                     reply A          -> reply B
    #[test]
    fn queued_ledger_read_waits_for_the_first_successful_pin() {
        let (alice, bob, mut link) = fixture();
        let tx = alice.tx_ref(begin(&alice));
        let query = alice.prepare_query(&Query::from("notes")).unwrap();
        let mut first = pin!(tx.ledger_read(&query));
        let mut second = pin!(tx.ledger_read(&query));
        assert!(poll(first.as_mut()).is_pending());
        assert!(poll(second.as_mut()).is_pending());
        let requests = link.requests(&alice);
        assert!(
            matches!(
                requests.as_slice(),
                [SyncMessage::Read {
                    span: ReadSpan::Once { at: None },
                    ..
                }]
            ),
            "only the first read may fetch before the snapshot is pinned"
        );
        link.reply(&bob, requests);
        block_on(alice.tick()).unwrap();
        assert!(poll(second.as_mut()).is_pending());
        assert!(
            link.requests(&alice).is_empty(),
            "a reply alone does not pin the snapshot"
        );
        let Poll::Ready(Ok(first)) = poll(first.as_mut()) else {
            panic!("the first read should pin the snapshot");
        };

        assert!(poll(second.as_mut()).is_pending());
        let requests = link.requests(&alice);
        assert!(matches!(
            requests.as_slice(),
            [SyncMessage::Read { span: ReadSpan::Once { at: Some(at) }, .. }] if *at == first.through
        ));
        link.reply(&bob, requests);
        block_on(alice.tick()).unwrap();
        let Poll::Ready(Ok(second)) = poll(second.as_mut()) else {
            panic!("the second read should use the pinned snapshot");
        };
        assert_eq!(second.through, first.through);
    }

    /// Alice's later query prepares successfully but fails serializable
    /// validation before hydration. Its error waits for Bob's earlier read.
    /// Alice: pin -> fetch A -> admit B -> return A -> return B's error
    /// Bob:                              reply A
    #[test]
    fn early_ledger_query_error_waits_for_the_earlier_read() {
        let (alice, bob, mut link) = fixture();
        let id = begin(&alice);
        let tx = alice.tx_ref(id);
        link.pin(&alice, &bob, id);
        let query = alice.prepare_query(&Query::from("notes")).unwrap();
        let join = alice
            .prepare_query(&Query::from("notes").flat_join(
                "history.notes",
                "notes.id",
                "history.notes.id",
            ))
            .unwrap();
        let mut first = pin!(tx.ledger_read(&query));
        let mut second = pin!(tx.ledger_read(&join));
        assert!(poll(first.as_mut()).is_pending());
        assert!(
            poll(second.as_mut()).is_pending(),
            "the early error must wait its turn"
        );
        let requests = link.requests(&alice);
        assert_eq!(requests.len(), 1);
        link.reply(&bob, requests);
        block_on(alice.tick()).unwrap();
        assert!(matches!(poll(first.as_mut()), Poll::Ready(Ok(_))));
        let Poll::Ready(Err(error)) = poll(second.as_mut()) else {
            panic!("a serializable ledger read should refuse the flat join");
        };
        assert_eq!(error.code, ErrorCode::Query);
        assert!(
            error
                .message
                .contains("not supported in serializable transactions")
        );
    }

    /// Ordinary serializable snapshots retain Alice's own pending commits
    /// from transaction start, even if Bob accepts one past the pinned cut.
    /// A ledger read must refuse that row once it has a newer position.
    /// Alice: insert -> begin -> pin with empty read -> read inserted row
    /// Bob:                       reply at cut -> accept insert
    #[test]
    fn ledger_read_refuses_own_commit_accepted_after_the_pinned_cut() {
        let (alice, bob, mut link) = fixture();
        let write = block_on(alice.insert("notes", cells("pending"), Default::default())).unwrap();
        let tx = alice.tx_ref(begin(&alice));
        block_on(alice.tick()).unwrap();
        let commit = loop {
            match link.client.try_recv().expect("Alice has sent her commit") {
                message @ SyncMessage::Commit { .. } => break message,
                message => link.server.send(message).unwrap(),
            }
        };

        let empty = alice
            .prepare_query(&Query::from("notes").filter(eq(col("text"), lit("missing"))))
            .unwrap();
        let mut first = pin!(tx.ledger_read(&empty));
        assert!(poll(first.as_mut()).is_pending());
        let requests = link.requests(&alice);
        assert_eq!(requests.len(), 1);
        link.reply(&bob, requests);
        block_on(alice.tick()).unwrap();
        let Poll::Ready(Ok(first)) = poll(first.as_mut()) else {
            panic!("the empty read should pin the cut");
        };
        assert!(first.rows.is_empty());

        link.reply(&bob, [commit]);
        block_on(alice.tick()).unwrap();
        let at = drive(
            &[&alice, &bob],
            alice.wait_for_transaction(write.mergeable_tx_id(), DurabilityTier::Global),
        )
        .unwrap();
        assert!(at > first.through);

        let query = alice
            .prepare_query(
                &Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(write.row_uuid().0)))),
            )
            .unwrap();
        let mut second = pin!(tx.ledger_read(&query));
        assert!(poll(second.as_mut()).is_pending());
        let requests = link.requests(&alice);
        assert_eq!(requests.len(), 1);
        link.reply(&bob, requests);
        block_on(alice.tick()).unwrap();
        let Poll::Ready(result) = poll(second.as_mut()) else {
            panic!("Bob answered the read at the pinned cut");
        };
        let error = result.expect_err("the row was accepted after the ledger cut");
        assert_eq!(error.code, ErrorCode::NotObserved);
        assert!(
            error
                .message
                .contains("found a row not accepted at its cut")
        );
    }

    /// Alice's first read refuses her staged row. Bob then inserts a row
    /// that Alice's first successful read sees at a fresh authority cut.
    #[test]
    fn failed_first_ledger_read_leaves_the_cut_unpinned() {
        let (alice, bob) = super::fixture();
        let tx = alice.tx_ref(begin(&alice));
        let pending = block_on(tx.insert("notes", cells("pending"), Default::default()))
            .unwrap()
            .row_uuid();
        let query = alice
            .prepare_query(&Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(pending.0)))))
            .unwrap();
        let error = drive(&[&alice, &bob], tx.ledger_read(&query)).unwrap_err();
        assert_eq!(error.code, ErrorCode::NotObserved);

        block_on(bob.insert("notes", cells("accepted"), Default::default())).unwrap();
        let query = alice
            .prepare_query(
                &Query::from("notes")
                    .filter(eq(col("text"), lit(Value::String("accepted".into())))),
            )
            .unwrap();
        let read = drive(&[&alice, &bob], tx.ledger_read(&query)).unwrap();
        assert_eq!(
            texts(read.rows.into_iter().map(|(row, _)| row).collect()),
            ["accepted"]
        );
    }

    /// Alice queues a second ledger read while the first fetch is in flight.
    /// Bob answers the first, then inserts a row. Once Alice refuses her
    /// staged row, the queued read fetches at a fresh cut and sees Bob's row.
    /// Alice: stage -> read A -> queue B -> refuse A -> fetch B
    /// Bob:                          reply A -> insert -> reply B
    #[test]
    fn queued_ledger_read_fetches_a_fresh_cut_after_the_first_read_fails() {
        let (alice, bob, mut link) = fixture();
        let tx = alice.tx_ref(begin(&alice));
        let pending = block_on(tx.insert("notes", cells("pending"), Default::default()))
            .unwrap()
            .row_uuid();
        let pending_query = alice
            .prepare_query(&Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(pending.0)))))
            .unwrap();
        let accepted_query = alice
            .prepare_query(
                &Query::from("notes")
                    .filter(eq(col("text"), lit(Value::String("accepted".into())))),
            )
            .unwrap();
        let mut first = pin!(tx.ledger_read(&pending_query));
        let mut second = pin!(tx.ledger_read(&accepted_query));
        assert!(poll(first.as_mut()).is_pending());
        assert!(poll(second.as_mut()).is_pending());
        let requests = link.requests(&alice);
        assert_eq!(requests.len(), 1, "only the first fetch can select a cut");
        link.reply(&bob, requests);
        block_on(alice.tick()).unwrap();
        let Poll::Ready(Err(error)) = poll(first.as_mut()) else {
            panic!("the first read should refuse the staged row");
        };
        assert_eq!(error.code, ErrorCode::NotObserved);

        block_on(bob.insert("notes", cells("accepted"), Default::default())).unwrap();
        assert!(poll(second.as_mut()).is_pending());
        let requests = link.requests(&alice);
        assert_eq!(requests.len(), 1);
        link.reply(&bob, requests);
        block_on(alice.tick()).unwrap();
        let Poll::Ready(Ok(read)) = poll(second.as_mut()) else {
            panic!("the queued read should see the accepted row");
        };
        assert_eq!(
            texts(read.rows.into_iter().map(|(row, _)| row).collect()),
            ["accepted"]
        );
    }

    /// Alice's two in-flight reads share a cut despite Bob's subsequent
    /// update. Alice's staged insert between admissions affects only her
    /// second read, which refuses the unsettled row.
    /// Alice: pin -> read A -> stage insert -> read B
    /// Bob:         update -> answer A and B at pinned cut
    #[test]
    fn overlapping_ledger_reads_share_a_cut_and_keep_their_staged_prefixes() {
        let (alice, bob, mut link) = fixture();
        let row = block_on(bob.insert("notes", cells("before"), Default::default()))
            .unwrap()
            .row_uuid();
        let id = begin(&alice);
        let tx = alice.tx_ref(id);
        let cut = link.pin(&alice, &bob, id);
        block_on(bob.update("notes", row, cells("after"), Default::default())).unwrap();

        let query = alice.prepare_query(&Query::from("notes")).unwrap();
        let mut first = pin!(tx.ledger_read(&query));
        assert!(poll(first.as_mut()).is_pending());
        block_on(tx.insert("notes", cells("unsettled"), Default::default())).unwrap();
        let mut second = pin!(tx.ledger_read(&query));
        assert!(poll(second.as_mut()).is_pending());
        let requests = link.requests(&alice);
        assert_eq!(
            requests.len(),
            2,
            "both reads must be sent before any reply"
        );
        assert!(requests.iter().all(|request| matches!(
            request,
            SyncMessage::Read { span: ReadSpan::Once { at: Some(at) }, .. } if *at == cut
        )));
        link.reply(&bob, requests);
        block_on(alice.tick()).unwrap();

        let Poll::Ready(Ok(first)) = poll(first.as_mut()) else {
            panic!("the earlier read should see the pinned accepted row");
        };
        assert_eq!(first.through, cut);
        assert_eq!(
            texts(first.rows.into_iter().map(|(row, _)| row).collect()),
            ["before"]
        );
        let Poll::Ready(Err(second)) = poll(second.as_mut()) else {
            panic!("the later read should see the unsettled insert and refuse it");
        };
        assert_eq!(second.code, ErrorCode::NotObserved);
    }

    /// Bob answers Alice's first of two overlapping reads. Alice still
    /// cannot commit until the second finishes, and reusing the ID cannot
    /// attach that read to the new transaction.
    #[test]
    fn commit_cannot_overtake_an_overlapping_ledger_read() {
        let (alice, bob, mut link) = fixture();
        let id = begin(&alice);
        let tx = alice.tx_ref(id);
        link.pin(&alice, &bob, id);
        block_on(tx.insert("notes", cells("staged"), Default::default())).unwrap();
        let query = alice.prepare_query(&Query::from("notes")).unwrap();
        let mut first = pin!(tx.ledger_read(&query));
        let mut second = pin!(tx.ledger_read(&query));
        assert!(poll(first.as_mut()).is_pending());
        assert!(poll(second.as_mut()).is_pending());
        let mut requests = link.requests(&alice);
        assert_eq!(requests.len(), 2, "both reads must be in flight");
        let unanswered = requests.pop().unwrap();
        link.reply(&bob, requests);
        block_on(alice.tick()).unwrap();
        assert!(matches!(poll(first.as_mut()), Poll::Ready(_)));
        assert!(poll(second.as_mut()).is_pending());

        let error = block_on(alice.commit_serializable_handle(id)).unwrap_err();
        assert_eq!(error.code, ErrorCode::NotObserved);
        block_on(alice.begin_serializable(id)).unwrap();
        link.reply(&bob, [unanswered]);
        block_on(alice.tick()).unwrap();
        assert!(matches!(poll(second.as_mut()), Poll::Ready(Err(_))));
        let rows = block_on(bob.all(&query, ReadOpts::default())).unwrap();
        assert!(rows.is_empty(), "the staged row must not publish");
    }

    /// Bob refuses Alice's second fetch before answering her first. The
    /// second read's error still waits for the first result, as does a
    /// still later query preparation failure.
    #[test]
    fn overlapping_ledger_read_errors_surface_in_admission_order() {
        let (alice, bob, mut link) = fixture();
        let id = begin(&alice);
        let tx = alice.tx_ref(id);
        link.pin(&alice, &bob, id);

        let query = alice.prepare_query(&Query::from("notes")).unwrap();
        let missing = Query::from("missing_table");
        let mut first = pin!(tx.ledger_read(&query));
        let mut second = pin!(tx.ledger_read(&query));
        let mut third = pin!(tx.ledger_read_query(&missing));
        assert!(poll(first.as_mut()).is_pending());
        assert!(poll(second.as_mut()).is_pending());
        assert!(poll(third.as_mut()).is_pending());
        let mut requests = link.requests(&alice);
        assert_eq!(requests.len(), 2, "both valid queries must be in flight");
        link.refuse(requests.pop().unwrap(), "later fetch refused");
        block_on(alice.tick()).unwrap();
        assert!(poll(second.as_mut()).is_pending());
        assert!(poll(third.as_mut()).is_pending());

        link.reply(&bob, requests);
        block_on(alice.tick()).unwrap();
        assert!(matches!(poll(first.as_mut()), Poll::Ready(Ok(_))));
        assert!(poll(third.as_mut()).is_pending());
        let Poll::Ready(Err(error)) = poll(second.as_mut()) else {
            panic!("expected the second read's authority refusal");
        };
        assert_eq!(error.message, "later fetch refused");
        assert!(matches!(poll(third.as_mut()), Poll::Ready(Err(_))));
    }
}

/// Alice's first read fetches Bob's chunked row, then fails: her replica has
/// none of its chunks yet. The failed read must leave her snapshot unpinned,
/// so her next read pins a fresh cut and sees the row Bob accepted since.
/// Alice: read A (fetch, then missing chunks) ------------> read B
/// Bob:          answer A                  -> insert "accepted" -> answer B
#[test]
fn failed_first_read_leaves_the_snapshot_unpinned() {
    let (alice, bob) = fixture();
    block_on(bob.insert(
        "notes",
        cells(&"chunked-é😀".repeat(10_000)),
        Default::default(),
    ))
    .unwrap();
    let id = begin(&alice);
    let tx = alice.tx_ref(id);
    let all = alice.prepare_query(&Query::from("notes")).unwrap();
    let error = drive(
        &[&alice, &bob],
        tx.all_prepared_at(&all, false, None, ReadTier::Remote),
    )
    .expect_err("the first read needs chunks Alice has not pulled yet");
    assert!(error.is_missing_chunks(), "{error:?}");

    let accepted = block_on(bob.insert("notes", cells("accepted"), Default::default()))
        .unwrap()
        .row_uuid();
    let query = alice
        .prepare_query(&Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(accepted.0)))))
        .unwrap();
    let rows = drive(
        &[&alice, &bob],
        tx.all_prepared_at(&query, false, None, ReadTier::Remote),
    )
    .unwrap();
    assert_eq!(
        texts(rows),
        ["accepted"],
        "a read after a failed first read used the failed read's cut"
    );
}

/// Alice's replica holds row X (her own accepted insert) and no read keeps it
/// current. Bob deletes X, or moves it out of Alice's filter. In her next
/// serializable transaction, Alice's first read pins the snapshot; her second
/// is answered as of it, and must not return X as her replica last had it:
/// the answer's listing (SPEC 8 §8.4) leaves X out, so her replica drops its
/// stale copy (jazz#6399). The transaction then commits.
/// Alice: insert X -----------------> read A (pins) -> read "x" -> commit
/// Bob:   accept X -> delete/move X -> answer A      -> answer   -> accept
#[test]
fn a_pinned_read_leaves_out_a_row_that_left_its_result_before_the_snapshot() {
    for delete in [true, false] {
        let (alice, bob) = fixture();
        let a = block_on(bob.insert("notes", cells("a"), Default::default()))
            .unwrap()
            .row_uuid();
        let x = block_on(alice.insert("notes", cells("x"), Default::default()))
            .unwrap()
            .row_uuid();
        exchange(&alice, &bob);
        let by_text = alice
            .prepare_query(&Query::from("notes").filter(eq(col("text"), lit(Value::from("x")))))
            .unwrap();
        assert_eq!(
            texts(block_on(alice.all(&by_text, ReadOpts::default())).unwrap()),
            ["x"],
            "Alice's replica holds X"
        );
        if delete {
            block_on(bob.delete("notes", x, Default::default())).unwrap();
        } else {
            block_on(bob.update("notes", x, cells("moved"), Default::default())).unwrap();
        }
        exchange(&alice, &bob);

        let id = begin(&alice);
        let tx = alice.tx_ref(id);
        let qa = alice
            .prepare_query(&Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(a.0)))))
            .unwrap();
        let first = drive(
            &[&alice, &bob],
            tx.all_prepared_at(&qa, false, None, ReadTier::Remote),
        )
        .unwrap();
        assert_eq!(texts(first), ["a"]);
        let second = drive(
            &[&alice, &bob],
            tx.all_prepared_at(&by_text, false, None, ReadTier::Remote),
        )
        .unwrap();
        assert!(
            second.is_empty(),
            "a pinned read returned a row that left its result before the snapshot \
             (delete: {delete}): {:?}",
            texts(second)
        );
        let by_id = alice
            .prepare_query(&Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(x.0)))))
            .unwrap();
        let third = drive(
            &[&alice, &bob],
            tx.all_prepared_at(&by_id, false, None, ReadTier::Remote),
        )
        .unwrap();
        let expected: &[&str] = if delete { &[] } else { &["moved"] };
        assert_eq!(texts(third), expected, "delete: {delete}");

        block_on(tx.insert("notes", cells("written"), Default::default())).unwrap();
        let receipt = block_on(alice.commit_serializable_handle(id))
            .unwrap_or_else(|error| panic!("commit failed (delete: {delete}): {error:?}"));
        drive(
            &[&alice, &bob],
            alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap_or_else(|error| panic!("commit refused (delete: {delete}): {error:?}"));
    }
}
