//! Abandoned reads release only their own request, including queued transport work.

use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, ReadOpts, ReadTier, RemoteLinkHint, SerializedReadResult,
    SyncMessage, Transport, TransportError, block_on, duplex,
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
use std::task::{Context, Poll, Waker};

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
        if let SyncMessage::Read {
            id,
            span: jazz::db::ReadSpan::Once { .. },
            ..
        } = &message
        {
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
fn assert_rows(result: SerializedReadResult) {
    let SerializedReadResult::Rows(rows) = result else {
        panic!("ordinary read")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell("body"), Some(Value::String("hello".into())));
}

/// Alice cancels one transaction read while Bob's transport is backpressured.
/// Her sibling read still receives Bob's row. Recording the public transport
/// proves the cancelled request never crosses the link after capacity returns.
#[test]
fn cancelled_transaction_request_leaves_sibling_read_intact() {
    let (alice, bob, gate) = fixture();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let first = alice.tx_ref(begin(&alice));
    let second = alice.tx_ref(begin(&alice));
    let mut cancelled = Box::pin(first.all_prepared_at(&query, false, None, ReadTier::Remote));
    let mut sibling = Box::pin(second.all_prepared_at(&query, false, None, ReadTier::Remote));
    assert!(poll(cancelled.as_mut()).is_pending());
    assert!(poll(sibling.as_mut()).is_pending());
    tick(&alice, &bob);
    drop(cancelled);
    gate.blocked.set(false);
    let rows = finish(&alice, &bob, sibling.as_mut()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell("body"), Some(Value::String("hello".into())));
    assert_eq!(
        gate.sent.borrow().len(),
        1,
        "only the surviving read may be sent"
    );
}

/// Alice cancels a binding one-shot before Bob can accept its Fetch.
/// Transport recording is needed to distinguish cleanup from an ignored answer.
#[test]
fn cancelled_binding_read_removes_queued_fetch() {
    let (alice, bob, gate) = fixture();
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = Box::pin(alice.all_serialized_query(
        &query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        None,
    ));
    assert!(poll(read.as_mut()).is_pending());
    tick(&alice, &bob);
    drop(read);
    gate.blocked.set(false);
    for _ in 0..3 {
        tick(&alice, &bob);
    }
    assert!(gate.sent.borrow().is_empty(), "cancelled read reached Bob");
}

/// Alice's exhausted coverage budget releases a request queued for Bob.
/// The public transport exposes the otherwise invisible abandoned work.
#[test]
fn coverage_timeout_removes_queued_fetch() {
    let (alice, bob, gate) = fixture();
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let expired = Cell::new(false);
    let budget = || expired.get();
    let mut read = Box::pin(alice.all_serialized_query(
        &query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        Some(&budget),
    ));
    assert!(poll(read.as_mut()).is_pending());
    tick(&alice, &bob);
    expired.set(true);
    let Poll::Ready(Err(error)) = poll(read.as_mut()) else {
        panic!("expected coverage refusal")
    };
    assert_eq!(error.code, ErrorCode::NotObserved);
    // Keep the completed future alive: cleanup belongs to completion too.
    gate.blocked.set(false);
    for _ in 0..3 {
        tick(&alice, &bob);
    }
    assert!(gate.sent.borrow().is_empty(), "expired read reached Bob");
}

/// Alice falls back locally when Bob's link fails during a best-effort
/// transaction read. Recording sends proves its queued Fetch is released.
#[test]
fn local_fallback_releases_unsent_transaction_fetch() {
    let (alice, bob, gate) = fixture();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let transaction = alice.tx_ref(begin(&alice));
    let mut read = Box::pin(transaction.all_prepared_at(&query, false, None, ReadTier::LocalFirst));
    assert!(poll(read.as_mut()).is_pending());
    tick(&alice, &bob);
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    let Poll::Ready(Ok(rows)) = poll(read.as_mut()) else {
        panic!("expected local fallback")
    };
    assert!(rows.is_empty());
    gate.blocked.set(false);
    alice.set_remote_link_hint(RemoteLinkHint::Live);
    for _ in 0..3 {
        tick(&alice, &bob);
    }
    assert!(
        gate.sent.borrow().is_empty(),
        "completed fallback left its request queued"
    );
}

/// Alice's Remote read retries after reconnecting to Bob, retaining upstream's
/// ordinary retry contract. Only the new connection's Fetch crosses the link;
/// transport recording distinguishes it from a stale queued request.
#[test]
fn reconnect_replaces_old_queued_fetch() {
    let (alice, bob, gate) = fixture();
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = Box::pin(alice.all_serialized_query(
        &query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        None,
    ));
    assert!(poll(read.as_mut()).is_pending());
    tick(&alice, &bob);
    reconnect(&alice, &bob, &gate);
    assert!(poll(read.as_mut()).is_pending());
    gate.blocked.set(false);
    assert_rows(finish(&alice, &bob, read.as_mut()).unwrap());
    assert_eq!(
        gate.sent.borrow().len(),
        1,
        "the old queued Fetch survived reconnect"
    );
}

/// Alice cancels a read already sent to Bob; its late reply must not complete
/// her subsequent read. Pausing Bob's public tick controls answer delivery.
#[test]
fn late_cancelled_reply_does_not_complete_sibling() {
    let (alice, bob, gate) = fixture();
    gate.blocked.set(false);
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut cancelled = Box::pin(alice.all_serialized_query(
        &query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        None,
    ));
    assert!(poll(cancelled.as_mut()).is_pending());
    block_on(alice.tick()).unwrap();
    assert_eq!(gate.sent.borrow().len(), 1);
    drop(cancelled);
    let mut sibling = Box::pin(alice.all_serialized_query(
        &query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        None,
    ));
    assert!(poll(sibling.as_mut()).is_pending());
    block_on(bob.tick()).unwrap();
    block_on(alice.tick()).unwrap();
    assert!(
        poll(sibling.as_mut()).is_pending(),
        "another read's answer completed this one"
    );
    assert_rows(finish(&alice, &bob, sibling.as_mut()).unwrap());
    assert_eq!(gate.sent.borrow().len(), 2);
}

/// Alice's coverage callback cancels another read while the database evaluates
/// Bob's answerability. Cancellation must not panic under that borrowed store,
/// and only Alice's surviving request may reach Bob when capacity returns.
#[test]
fn cancellation_inside_coverage_callback_defers_fetch_cleanup() {
    let (alice, bob, gate) = fixture();
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut first = Box::pin(alice.all_serialized_query(
        &query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        None,
    ));
    assert!(poll(first.as_mut()).is_pending());
    let cancelled = RefCell::new(Some(first));
    let budget = || {
        drop(cancelled.borrow_mut().take());
        false
    };
    let mut sibling = Box::pin(alice.all_serialized_query(
        &query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        Some(&budget),
    ));
    assert!(poll(sibling.as_mut()).is_pending());
    assert!(cancelled.borrow().is_none());
    gate.blocked.set(false);
    assert_rows(finish(&alice, &bob, sibling.as_mut()).unwrap());
    assert_eq!(gate.sent.borrow().len(), 1, "cancelled read reached Bob");
}
