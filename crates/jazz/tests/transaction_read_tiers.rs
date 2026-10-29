//! Explicit local transaction reads use the replica without asking its authority.

use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, Error, ReadOpts, ReadTier, RemoteLinkHint,
    SerializedReadResult, SyncMessage, Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

fn fixture() -> (Db, Db) {
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
    let bob = open(2, AuthorSubject::SYSTEM);
    bob.set_authority();
    block_on(bob.insert("notes", cells("authority"), Default::default())).unwrap();
    let alice = open(1, AuthorSubject::for_test_bytes([1; 16]));
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    (alice, bob)
}

fn cells(body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".into(), Value::String(body.into()))])
}

struct CountFetches {
    transport: Box<dyn Transport>,
    sent: Rc<Cell<usize>>,
}
impl Transport for CountFetches {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if matches!(message, SyncMessage::Fetch { .. }) {
            self.sent.set(self.sent.get() + 1);
        }
        self.transport.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.transport.try_recv()
    }
}
fn connect(alice: &Db, bob: &Db) -> Rc<Cell<usize>> {
    let (up, down) = duplex();
    let sent = Rc::new(Cell::new(0));
    block_on(alice.connect_upstream(Box::new(CountFetches {
        transport: up,
        sent: sent.clone(),
    })));
    bob.accept_subscriber(down, AuthorSubject::for_test_bytes([1; 16]));
    alice.set_remote_link_hint(RemoteLinkHint::Live);
    sent
}
fn begin(alice: &Db) -> OpenTransactionId {
    let tx = OpenTransactionId::new();
    block_on(alice.begin_serializable(tx)).unwrap();
    block_on(
        alice
            .tx_ref(tx)
            .insert("notes", cells("staged"), Default::default()),
    )
    .unwrap();
    tx
}
fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn drive<T>(alice: &Db, bob: &Db, future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..64 {
        if let Poll::Ready(value) = poll(future.as_mut()) {
            return value;
        }
        block_on(alice.tick()).unwrap();
        block_on(bob.tick()).unwrap();
    }
    panic!("read did not settle");
}
fn bodies(rows: Vec<CurrentRow>) -> Vec<String> {
    let mut result: Vec<_> = rows
        .into_iter()
        .map(|row| match row.cell("body") {
            Some(Value::String(body)) => body,
            other => panic!("expected body, got {other:?}"),
        })
        .collect();
    result.sort();
    result
}
async fn read(
    alice: &Db,
    tx: OpenTransactionId,
    tier: ReadTier,
    serialized: bool,
) -> Result<Vec<String>, Error> {
    let query = Query::from("notes");
    if serialized {
        let result = alice
            .all_serialized_query(
                &postcard::to_allocvec(&query).unwrap(),
                ReadOpts::for_read_tier(tier),
                Some(tx),
                None,
                None,
            )
            .await?;
        let SerializedReadResult::Rows(rows) = result else {
            panic!("ordinary query")
        };
        Ok(bodies(rows))
    } else {
        let prepared = alice.prepare_query(&query)?;
        Ok(bodies(
            alice
                .tx_ref(tx)
                .all_prepared_at(&prepared, false, None, tier)
                .await?,
        ))
    }
}

/// Alice's LocalOnly read returns her staging before Bob answers, through both
/// core and binding entry points. Recording the public transport is necessary
/// because returned rows alone cannot prove that no Fetch was sent to Bob.
#[test]
fn local_only_reads_finish_without_contacting_the_connected_authority() {
    for serialized in [false, true] {
        let (alice, bob) = fixture();
        let sent = connect(&alice, &bob);
        let tx = begin(&alice);
        let mut local = pin!(read(&alice, tx, ReadTier::LocalOnly, serialized));
        assert_eq!(
            poll(local.as_mut()).map(Result::unwrap),
            Poll::Ready(vec!["staged".into()])
        );
        for _ in 0..3 {
            block_on(alice.tick()).unwrap();
            block_on(bob.tick()).unwrap();
        }
        assert_eq!(sent.get(), 0);
        alice.abandon_transaction_handle(tx).unwrap();
    }
}

/// Alice's first local read fixes her snapshot. A later Remote read may obtain
/// coverage from Bob but cannot introduce rows absent from that first snapshot.
#[test]
fn a_local_only_read_pins_the_snapshot_before_later_remote_coverage() {
    let (alice, bob) = fixture();
    let sent = connect(&alice, &bob);
    let tx = begin(&alice);
    let mut local = pin!(read(&alice, tx, ReadTier::LocalOnly, false));
    assert_eq!(
        poll(local.as_mut()).map(Result::unwrap),
        Poll::Ready(vec!["staged".into()])
    );
    block_on(bob.insert("notes", cells("later authority"), Default::default())).unwrap();
    let rows = drive(&alice, &bob, read(&alice, tx, ReadTier::Remote, false)).unwrap();
    assert_eq!(rows, ["staged"]);
    assert!(sent.get() > 0);
    alice.abandon_transaction_handle(tx).unwrap();
}

/// Alice's Remote read remains pending offline and receives Bob's rows after
/// connection. An independent LocalFirst read can still use offline staging.
#[test]
fn remote_reads_still_require_authority_and_local_first_still_works_offline() {
    let (alice, bob) = fixture();
    let local_tx = begin(&alice);
    assert_eq!(
        block_on(read(&alice, local_tx, ReadTier::LocalFirst, true)).unwrap(),
        ["staged"]
    );
    alice.abandon_transaction_handle(local_tx).unwrap();
    let tx = begin(&alice);
    let mut remote = pin!(read(&alice, tx, ReadTier::Remote, true));
    assert!(poll(remote.as_mut()).is_pending());
    connect(&alice, &bob);
    assert_eq!(
        drive(&alice, &bob, remote.as_mut()).unwrap(),
        ["authority", "staged"]
    );
    alice.abandon_transaction_handle(tx).unwrap();
}
