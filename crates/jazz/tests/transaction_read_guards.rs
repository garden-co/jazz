//! In-flight transaction reads cannot be bypassed by commit or handle reuse.

use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, ReadOpts, ReadTier, RemoteLinkHint, SyncMessage,
    Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll, Wake, Waker};

#[derive(Clone, Default)]
struct Gate {
    blocked: Rc<Cell<bool>>,
    sent: Rc<RefCell<Vec<u64>>>,
}
struct GatedTransport {
    inner: Box<dyn Transport>,
    gate: Gate,
}
impl Transport for GatedTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if let SyncMessage::Fetch { id, .. } = &message {
            if self.gate.blocked.get() {
                return Err(TransportError::Backpressure);
            }
            self.gate.sent.borrow_mut().push(*id);
        }
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}
fn fixture() -> (Db, Db, Gate) {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let open = |node, author| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .unwrap()
    };
    let alice = open(1, AuthorSubject::for_test_bytes([1; 16]));
    let bob = open(2, AuthorSubject::SYSTEM);
    bob.set_authority();
    block_on(bob.insert(
        "notes",
        BTreeMap::from([("body".into(), Value::String("hello".into()))]),
        Default::default(),
    ))
    .unwrap();
    let gate = Gate::default();
    reconnect(&alice, &bob, &gate);
    for _ in 0..3 {
        tick(&alice, &bob);
    }
    gate.blocked.set(true);
    (alice, bob, gate)
}
fn reconnect(alice: &Db, bob: &Db, gate: &Gate) {
    let (up, down) = duplex();
    block_on(alice.connect_upstream(Box::new(GatedTransport {
        inner: up,
        gate: gate.clone(),
    })));
    bob.accept_subscriber(down, AuthorSubject::for_test_bytes([1; 16]));
    alice.set_remote_link_hint(RemoteLinkHint::Live);
}
fn begin(alice: &Db) -> OpenTransactionId {
    let tx = OpenTransactionId::new();
    block_on(alice.begin_serializable(tx)).unwrap();
    tx
}
fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn tick(alice: &Db, bob: &Db) {
    block_on(alice.tick()).unwrap();
    block_on(bob.tick()).unwrap();
}
fn finish<T>(alice: &Db, bob: &Db, mut future: Pin<&mut impl Future<Output = T>>) -> T {
    for _ in 0..32 {
        if let Poll::Ready(value) = poll(future.as_mut()) {
            return value;
        }
        tick(alice, bob);
    }
    panic!("read did not settle");
}
#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn poll_with_wake<T>(future: Pin<&mut impl Future<Output = T>>, wake: &Arc<WakeCount>) -> Poll<T> {
    future.poll(&mut Context::from_waker(&Waker::from(wake.clone())))
}
fn cells(body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".into(), Value::String(body.into()))])
}
fn bodies(rows: Vec<jazz::db::CurrentRow>) -> Vec<String> {
    let mut bodies = rows
        .into_iter()
        .map(|row| match row.cell("body") {
            Some(Value::String(body)) => body,
            other => panic!("invalid body: {other:?}"),
        })
        .collect::<Vec<_>>();
    bodies.sort();
    bodies
}

/// Alice starts a Remote transaction read, then tries to commit its staged
/// write before Bob answers. The commit must refuse and consume the handle,
/// wake the reader, and leave both replicas without the staged write.
#[test]
fn commit_cannot_overtake_an_unfinished_read() {
    let (alice, bob, gate) = fixture();
    let id = begin(&alice);
    let transaction = alice.tx_ref(id);
    block_on(transaction.insert("notes", cells("uncommitted"), Default::default())).unwrap();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut read = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::Remote));
    let wake = Arc::new(WakeCount::default());
    assert!(poll_with_wake(read.as_mut(), &wake).is_pending());
    let error = block_on(alice.commit_serializable_handle(id)).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert!(
        wake.0.load(Ordering::SeqCst) > 0,
        "closing the transaction did not wake its read"
    );
    let Poll::Ready(Err(error)) = poll(read.as_mut()) else {
        panic!("closed read is still waiting")
    };
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert!(block_on(alice.commit_serializable_handle(id)).is_err());
    gate.blocked.set(false);
    for _ in 0..4 {
        tick(&alice, &bob);
    }
    for db in [&alice, &bob] {
        let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
        let result = block_on(db.all_serialized_query(
            &query,
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            None,
            None,
            None,
        ))
        .unwrap();
        let jazz::db::SerializedReadResult::Rows(rows) = result else {
            panic!("ordinary read")
        };
        assert!(!bodies(rows).contains(&"uncommitted".to_owned()));
    }
    assert!(
        gate.sent.borrow().is_empty(),
        "closed transaction left a Fetch queued"
    );
}

/// Alice abandons a transaction while Bob has not answered. Her executor must
/// be woken without another network event, and the read must fail immediately.
#[test]
fn abandon_wakes_and_refuses_pending_read() {
    let (alice, _, _) = fixture();
    let id = begin(&alice);
    let transaction = alice.tx_ref(id);
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut read = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::Remote));
    let wake = Arc::new(WakeCount::default());
    assert!(poll_with_wake(read.as_mut(), &wake).is_pending());
    alice.abandon_transaction_handle(id).unwrap();
    assert!(
        wake.0.load(Ordering::SeqCst) > 0,
        "abandonment did not wake its pending read"
    );
    let Poll::Ready(Err(error)) = poll(read.as_mut()) else {
        panic!("abandoned read is still waiting")
    };
    assert_eq!(error.code, ErrorCode::NotObserved);
}

/// Alice reuses an abandoned transaction's ID before Bob replies. The old
/// future must fail instead of observing or validating the replacement batch.
#[test]
fn reused_handle_does_not_adopt_the_old_read() {
    let (alice, bob, gate) = fixture();
    let id = begin(&alice);
    let transaction = alice.tx_ref(id);
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut old = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::Remote));
    assert!(poll(old.as_mut()).is_pending());
    alice.abandon_transaction_handle(id).unwrap();
    block_on(alice.begin_serializable(id)).unwrap();
    block_on(transaction.insert("notes", cells("replacement"), Default::default())).unwrap();
    let Poll::Ready(Err(error)) = poll(old.as_mut()) else {
        panic!("old read adopted a reused handle")
    };
    assert_eq!(error.code, ErrorCode::NotObserved);
    gate.blocked.set(false);
    let mut new = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::Remote));
    assert_eq!(
        bodies(finish(&alice, &bob, new.as_mut()).unwrap()),
        vec!["hello", "replacement"]
    );
    assert_eq!(gate.sent.borrow().len(), 1);
}

/// Alice starts two reads before either returns. Bob writes between their
/// round trips: both reads must use the first read's accepted snapshot.
/// Pausing public ticks establishes the order without private state hooks.
#[test]
fn concurrent_reads_pin_one_snapshot_in_admission_order() {
    let (alice, bob, gate) = fixture();
    gate.blocked.set(false);
    let transaction = alice.tx_ref(begin(&alice));
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut first = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::Remote));
    assert!(poll(first.as_mut()).is_pending());
    tick(&alice, &bob); // Bob has answered the first read; Alice has not polled it.
    block_on(bob.insert("notes", cells("later"), Default::default())).unwrap();
    let mut second = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::Remote));
    assert!(poll(second.as_mut()).is_pending());
    tick(&alice, &bob);
    assert_eq!(
        gate.sent.borrow().len(),
        1,
        "second read overtook snapshot selection"
    );
    assert_eq!(
        bodies(finish(&alice, &bob, first.as_mut()).unwrap()),
        vec!["hello"]
    );
    assert_eq!(
        bodies(finish(&alice, &bob, second.as_mut()).unwrap()),
        vec!["hello"]
    );
    assert_eq!(gate.sent.borrow().len(), 2);
}

/// Alice cancels the leading read. Her queued sibling wakes, retains the
/// staging prefix captured when it began, and still completes through Bob.
#[test]
fn cancelling_leader_wakes_next_read_without_widening_its_prefix() {
    let (alice, bob, gate) = fixture();
    let transaction = alice.tx_ref(begin(&alice));
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    block_on(transaction.insert("notes", cells("first"), Default::default())).unwrap();
    let mut first = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::Remote));
    assert!(poll(first.as_mut()).is_pending());
    block_on(transaction.insert("notes", cells("middle"), Default::default())).unwrap();
    let mut second = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::Remote));
    let wake = Arc::new(WakeCount::default());
    assert!(poll_with_wake(second.as_mut(), &wake).is_pending());
    block_on(transaction.insert("notes", cells("last"), Default::default())).unwrap();
    drop(first);
    assert!(
        wake.0.load(Ordering::SeqCst) > 0,
        "cancellation did not release the next read"
    );
    gate.blocked.set(false);
    assert_eq!(
        bodies(finish(&alice, &bob, second.as_mut()).unwrap()),
        vec!["first", "hello", "middle"]
    );
    assert_eq!(gate.sent.borrow().len(), 1);
}

/// Alice cancels an ordinary read before using its result. That releases its
/// guard without poisoning the transaction; her independent staged write may
/// still commit and reach Bob through normal synchronization.
#[test]
fn cancelled_ordinary_read_does_not_poison_commit() {
    let (alice, bob, gate) = fixture();
    let id = begin(&alice);
    let transaction = alice.tx_ref(id);
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    block_on(transaction.insert("notes", cells("kept"), Default::default())).unwrap();
    let mut read = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::Remote));
    assert!(poll(read.as_mut()).is_pending());
    drop(read);
    block_on(alice.commit_serializable_handle(id)).unwrap();
    gate.blocked.set(false);
    for _ in 0..6 {
        tick(&alice, &bob);
    }
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let jazz::db::SerializedReadResult::Rows(rows) = block_on(bob.all_serialized_query(
        &query,
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
        None,
        None,
        None,
    ))
    .unwrap() else {
        panic!("ordinary read")
    };
    assert_eq!(bodies(rows), vec!["hello", "kept"]);
    assert!(gate.sent.borrow().is_empty());
}

/// Alice's storage owner retires while one read awaits Bob and another awaits
/// the first. Both executor tasks must wake and refuse without network traffic.
#[test]
fn retirement_wakes_issued_and_queued_reads() {
    let (alice, _, _) = fixture();
    let transaction = alice.tx_ref(begin(&alice));
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut first = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::Remote));
    let mut second = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::Remote));
    let first_wake = Arc::new(WakeCount::default());
    let second_wake = Arc::new(WakeCount::default());
    assert!(poll_with_wake(first.as_mut(), &first_wake).is_pending());
    assert!(poll_with_wake(second.as_mut(), &second_wake).is_pending());
    alice.retire_storage_owner().unwrap();
    assert!(first_wake.0.load(Ordering::SeqCst) > 0);
    assert!(second_wake.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(poll(first.as_mut()), Poll::Ready(Err(_))));
    assert!(matches!(poll(second.as_mut()), Poll::Ready(Err(_))));
}
