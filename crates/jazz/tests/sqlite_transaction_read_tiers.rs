//! Binding transaction reads retain their requested coverage and read lifecycle.
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ErrorCode, ReadOpts, ReadTier, RemoteLinkHint,
    SerializedReadResult, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use std::{
    cell::Cell,
    collections::BTreeMap,
    future::Future,
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
};

fn fixture() -> (Db, Db) {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True),
                    ),
            )
            .build(),
    )
    .unwrap();
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
    bob.initialize_authority_catalogue().unwrap();
    insert(&bob, "authority");
    let alice = open(1, AuthorSubject::for_test_bytes([1; 16]));
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    (alice, bob)
}
fn cells(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("text".into(), Value::String(text.into()))])
}
fn insert(db: &Db, text: &str) {
    block_on(db.insert("notes", cells(text), Default::default())).unwrap();
}
fn begin(db: &Db) -> OpenTransactionId {
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    tx
}
fn connect(alice: &Db, bob: &Db) {
    let (up, down) = duplex();
    block_on(alice.connect_upstream(up));
    bob.accept_subscriber(down, AuthorSubject::for_test_bytes([1; 16]));
    alice.set_remote_link_hint(RemoteLinkHint::Live);
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
    panic!("read did not finish");
}
async fn read(
    db: &Db,
    tx: Option<OpenTransactionId>,
    tier: ReadTier,
    expired: &Cell<bool>,
) -> Result<Vec<String>, Error> {
    let result = db
        .all_serialized_query_as(
            &postcard::to_allocvec(&Query::from("notes")).unwrap(),
            ReadOpts::for_read_tier(tier),
            tx,
            None,
            None,
            true,
            || expired.get(),
            |_| {},
        )
        .await?;
    let SerializedReadResult::Rows(rows) = result else {
        panic!("ordinary query")
    };
    let mut texts = rows
        .iter()
        .map(|row| {
            let (_, Some(Value::String(text))) =
                row.cells().find(|(name, _)| *name == "text").unwrap()
            else {
                panic!("text cell")
            };
            text.clone()
        })
        .collect::<Vec<_>>();
    texts.sort();
    Ok(texts)
}

/// Alice starts a Remote read offline; her independent LocalFirst read remains
/// available. Bob's answer hydrates only the admitted staged prefix, and a later
/// Remote read keeps that first authority snapshot despite Bob's later insert.
/// Alice: staged A -> read -> staged B -> connect -> read -> Bob inserts -> read.
#[test]
fn offline_remote_read_retains_prefix_and_pinned_snapshot() {
    let (alice, bob) = fixture();
    let tx = begin(&alice);
    let transaction = alice.tx_ref(tx);
    block_on(transaction.insert("notes", cells("first"), Default::default())).unwrap();
    let expired = Cell::new(false);
    let mut pending = pin!(read(&alice, Some(tx), ReadTier::Remote, &expired));
    assert!(poll(pending.as_mut()).is_pending());
    block_on(transaction.insert("notes", cells("second"), Default::default())).unwrap();
    assert_eq!(
        block_on(read(&alice, None, ReadTier::LocalFirst, &expired)).unwrap(),
        Vec::<String>::new()
    );
    assert!(poll(pending.as_mut()).is_pending());
    connect(&alice, &bob);
    assert_eq!(
        drive(&alice, &bob, pending.as_mut()).unwrap(),
        ["authority", "first"]
    );
    insert(&bob, "later authority");
    assert_eq!(
        drive(
            &alice,
            &bob,
            read(&alice, Some(tx), ReadTier::Remote, &expired)
        )
        .unwrap(),
        ["authority", "first", "second"]
    );
    alice.abandon_transaction_handle(tx).unwrap();
}

/// Alice cancels her offline read and releases its queued successor. Only the
/// successor's staged prefix is visible when Bob finally supplies coverage.
#[test]
fn cancellation_releases_the_next_offline_read() {
    let (alice, bob) = fixture();
    let tx = begin(&alice);
    let transaction = alice.tx_ref(tx);
    let expired = Cell::new(false);
    let mut first = Box::pin(read(&alice, Some(tx), ReadTier::Remote, &expired));
    assert!(poll(first.as_mut()).is_pending());
    block_on(transaction.insert("notes", cells("second"), Default::default())).unwrap();
    let mut second = pin!(read(&alice, Some(tx), ReadTier::Remote, &expired));
    assert!(poll(second.as_mut()).is_pending());
    block_on(transaction.insert("notes", cells("third"), Default::default())).unwrap();
    drop(first);
    assert!(poll(second.as_mut()).is_pending());
    connect(&alice, &bob);
    assert_eq!(
        drive(&alice, &bob, second.as_mut()).unwrap(),
        ["authority", "second"]
    );
    block_on(alice.commit_serializable_handle(tx)).unwrap();
}

/// Alice's offline read cannot survive rollback or move into a reused handle;
/// her binding's coverage timeout also releases the ordinary read guard.
#[test]
fn rollback_and_coverage_expiry_release_offline_reads() {
    for end in ["rollback", "commit", "expiry"] {
        let (alice, _bob) = fixture();
        let tx = begin(&alice);
        let expired = Cell::new(false);
        let mut pending = pin!(read(&alice, Some(tx), ReadTier::Remote, &expired));
        assert!(poll(pending.as_mut()).is_pending());
        match end {
            "rollback" => {
                alice.abandon_transaction_handle(tx).unwrap();
                block_on(alice.begin_serializable(tx)).unwrap();
            }
            "commit" => {
                let error = block_on(alice.commit_serializable_handle(tx)).unwrap_err();
                assert_eq!(error.code, ErrorCode::NotObserved);
                block_on(alice.begin_serializable(tx)).unwrap();
            }
            _ => expired.set(true),
        }
        let Poll::Ready(result) = poll(pending.as_mut()) else {
            panic!("closed or expired read stayed pending")
        };
        assert_eq!(result.unwrap_err().code, ErrorCode::NotObserved);
        block_on(alice.commit_serializable_handle(tx)).unwrap();
    }
}

/// Alice's explicit LocalOnly transaction read must finish from her staged
/// rows even while connected to Bob, before his transport has been driven.
#[test]
fn local_only_transaction_read_never_waits_for_authority() {
    let (alice, bob) = fixture();
    connect(&alice, &bob);
    let tx = begin(&alice);
    block_on(
        alice
            .tx_ref(tx)
            .insert("notes", cells("staged"), Default::default()),
    )
    .unwrap();
    let expired = Cell::new(false);
    let mut local = pin!(read(&alice, Some(tx), ReadTier::LocalOnly, &expired));
    assert_eq!(
        poll(local.as_mut()).map(Result::unwrap),
        Poll::Ready(vec!["staged".into()])
    );
    alice.abandon_transaction_handle(tx).unwrap();
}

/// Alice has already issued a Remote request to Bob. Losing that connection
/// must refuse the read, rather than return an unconfirmed empty local answer.
#[test]
fn issued_remote_read_does_not_fall_back_after_connection_loss() {
    for reconnect in [false, true] {
        let (alice, bob) = fixture();
        connect(&alice, &bob);
        let tx = begin(&alice);
        let expired = Cell::new(false);
        let mut pending = pin!(read(&alice, Some(tx), ReadTier::Remote, &expired));
        assert!(poll(pending.as_mut()).is_pending());
        if reconnect {
            connect(&alice, &bob);
        } else {
            alice.set_remote_link_hint(RemoteLinkHint::Failed);
        }
        let Poll::Ready(result) = poll(pending.as_mut()) else {
            panic!("issued request silently retried")
        };
        assert_eq!(result.unwrap_err().code, ErrorCode::NotObserved);
        alice.abandon_transaction_handle(tx).unwrap();
    }
}
