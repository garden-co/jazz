//! Explicit open-transaction identities survive schema views, staging and sync.

use crate::common::{compile_schema, session_eq};
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, LargeValueUpdate,
    LargeValueUpdatePage, LargeValueUpdateSplice, ReadOpts, ReadTier, RemoteLinkHint, SyncMessage,
    Transport, TransportError, WriteIdentity, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz::tx::DurabilityTier;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

fn author(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}
fn claims(account: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("\0claims:account".into(), Value::String(account.into()))])
}
fn cells(account: &str, body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("owner".into(), Value::String(account.into())),
        ("body".into(), Value::String(body.into())),
    ])
}
fn body(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".into(), Value::String(text.into()))])
}
fn schema() -> JazzSchema {
    let own = session_eq("owner", &["claims", "account"]);
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(own.clone())
                            .with_insert(own.clone())
                            .with_update(Some(own.clone()), own.clone())
                            .with_delete(own),
                    ),
            )
            .build(),
    )
}
fn open(node: u8, identity: AuthorSubject) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: identity,
        },
    )))
    .unwrap();
    for name in ["alice", "bob", "mallory"] {
        db.set_identity_claims(author(name), claims(name));
    }
    db
}
fn seed(db: &Db) -> (RowUuid, RowUuid) {
    let insert = |name| {
        block_on(db.insert("notes", cells(name, name), Default::default()))
            .unwrap()
            .row_uuid()
    };
    (insert("alice"), insert("bob"))
}
fn begin(db: &Db, serializable: bool, identity: AuthorSubject) -> OpenTransactionId {
    let tx = OpenTransactionId::new();
    if serializable {
        block_on(db.begin_serializable_for_identity(tx, identity)).unwrap();
    } else {
        block_on(db.begin_mergeable_for_identity(tx, identity)).unwrap();
    }
    tx
}
fn bodies(rows: Vec<CurrentRow>) -> Vec<String> {
    let mut values: Vec<_> = rows
        .into_iter()
        .map(|row| match row.cell("body") {
            Some(Value::String(value)) => value,
            other => panic!("expected body, got {other:?}"),
        })
        .collect();
    values.sort();
    values
}
fn read(db: &Db, tx: OpenTransactionId) -> Vec<String> {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    bodies(
        block_on(
            db.tx_ref(tx)
                .all_prepared_at(&query, false, None, ReadTier::LocalOnly),
        )
        .unwrap(),
    )
}
fn commit(
    db: &Db,
    tx: OpenTransactionId,
    serializable: bool,
) -> Result<jazz::db::TxReceipt, jazz::db::Error> {
    if serializable {
        block_on(db.commit_serializable_handle(tx))
    } else {
        block_on(db.commit_mergeable_handle(tx))
    }
}

/// Alice's bound batch keeps her read policy and row provenance through a fresh
/// schema handle; Bob's rows remain hidden for both transaction kinds.
#[test]
fn identity_is_shared_by_schema_views_and_inherited_by_writes() {
    for serializable in [false, true] {
        let db = open(1, AuthorSubject::SYSTEM);
        seed(&db);
        let view = block_on(db.register_schema_view(schema())).unwrap();
        let tx = begin(&db, serializable, author("alice"));
        assert_eq!(read(&view, tx), ["alice"]);
        block_on(
            view.tx_ref(tx)
                .insert("notes", cells("alice", "new"), Default::default()),
        )
        .unwrap();
        assert_eq!(read(&db, tx), ["alice", "new"]);
        commit(&view, tx, serializable).unwrap();
        let query = db
            .prepare_query(&Query::from("notes").select(["body", "$createdBy.identity.subject"]))
            .unwrap();
        let rows = block_on(db.all(&query, ReadOpts::default())).unwrap();
        let new = rows
            .iter()
            .find(|row| row.cell("body") == Some(Value::String("new".into())))
            .unwrap();
        assert_eq!(
            new.cell("$createdBy.identity.subject"),
            Some(Value::Nullable(Some(Box::new(Value::String(
                "alice".into()
            )))))
        );
    }
}

/// Alice cannot change a bound batch to Bob or to backend attribution, through
/// write options, explicit read identity or per-request claims.
#[test]
fn bound_transactions_reject_identity_and_mode_changes() {
    let db = open(1, AuthorSubject::SYSTEM);
    seed(&db);
    let tx = begin(&db, false, author("alice"));
    for identity in [
        WriteIdentity::Session(author("bob")),
        WriteIdentity::Attribution(author("alice")),
    ] {
        let err = block_on(db.tx_ref(tx).insert(
            "notes",
            cells("bob", "forged"),
            InsertOptions {
                identity,
                ..Default::default()
            },
        ))
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::WriteRejected);
    }
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    let err = block_on(
        db.tx_ref(tx)
            .all_prepared_as(&query, false, Some(author("bob"))),
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::WriteRejected);
    let query = query.with_identity_claims(author("bob"), claims("bob"));
    let err = block_on(db.tx_ref(tx).all_prepared(&query)).unwrap_err();
    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert_eq!(read(&db, tx), ["alice"]);
    db.abandon_transaction_handle(tx).unwrap();
}

/// Alice stages a permitted insert, then loses her claim before committing.
/// With no preceding read to invalidate, the commit must recheck write policy.
#[test]
fn commit_rechecks_revoked_session_permissions_without_a_read() {
    for serializable in [false, true] {
        let db = open(1, AuthorSubject::SYSTEM);
        let tx = begin(&db, serializable, author("alice"));
        block_on(
            db.tx_ref(tx)
                .insert("notes", cells("alice", "revoked"), Default::default()),
        )
        .unwrap();
        db.set_identity_claims(author("alice"), BTreeMap::new());
        let error = commit(&db, tx, serializable).unwrap_err();
        assert_eq!(error.code, ErrorCode::WriteRejected);
        let query = db.prepare_query(&Query::from("notes")).unwrap();
        assert!(
            block_on(db.all(&query, ReadOpts::default()))
                .unwrap()
                .is_empty()
        );
    }
}

/// Bob's hidden changes do not invalidate Alice's serializable read; a change
/// to Alice's visible row does invalidate it.
#[test]
fn serializable_readsets_use_the_bound_session_policy() {
    for change_alice in [false, true] {
        let db = open(1, AuthorSubject::SYSTEM);
        let (alice, bob) = seed(&db);
        let tx = begin(&db, true, author("alice"));
        assert_eq!(read(&db, tx), ["alice"]);
        block_on(db.update(
            "notes",
            if change_alice { alice } else { bob },
            body("changed"),
            Default::default(),
        ))
        .unwrap();
        let result = commit(&db, tx, true);
        if change_alice {
            assert_eq!(result.unwrap_err().code, ErrorCode::TransactionConflict);
        } else {
            result.unwrap();
        }
    }
}

/// Alice's partial edit cannot inspect Bob's hidden cell, even to report its
/// length. Her own edit still works and uses her provenance.
#[test]
fn partial_large_value_edits_apply_identity_before_reading_the_cell() {
    let db = open(1, AuthorSubject::SYSTEM);
    let (alice, bob) = seed(&db);
    let tx = begin(&db, false, author("alice"));
    let edits = |to| {
        vec![LargeValueUpdate::Splice {
            column: "body".into(),
            within: LargeValueUpdatePage::TextUtf16 { from: 0, to },
            splices: vec![LargeValueUpdateSplice {
                at: 0,
                delete: 1,
                insert: b"A".to_vec(),
            }],
        }]
    };
    let err = block_on(db.tx_ref(tx).update_with_large_value_mutations(
        "notes",
        bob,
        BTreeMap::new(),
        edits(500),
        Default::default(),
    ))
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::NotObserved);
    block_on(db.tx_ref(tx).update_with_large_value_mutations(
        "notes",
        alice,
        BTreeMap::new(),
        edits(5),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(read(&db, tx), ["Alice"]);
    commit(&db, tx, false).unwrap();
}

/// The backend may attribute to Alice while reading Bob's rows. An ordinary
/// Alice database cannot open that backend-only mode, and a failed begin does
/// not consume the transaction handle.
#[test]
fn backend_attribution_is_explicit_and_requires_system_identity() {
    for serializable in [false, true] {
        let alice = open(1, author("alice"));
        let tx = OpenTransactionId::new();
        let error = if serializable {
            block_on(alice.begin_serializable_for_attribution(tx, author("bob")))
        } else {
            block_on(alice.begin_mergeable_for_attribution(tx, author("bob")))
        }
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::Query);
        assert!(error.message.contains("SYSTEM backend"));
        block_on(alice.begin_mergeable(tx)).unwrap();
        alice.abandon_transaction_handle(tx).unwrap();
        let backend = open(2, AuthorSubject::SYSTEM);
        seed(&backend);
        if serializable {
            block_on(backend.begin_serializable_for_attribution(tx, author("alice"))).unwrap();
        } else {
            block_on(backend.begin_mergeable_for_attribution(tx, author("alice"))).unwrap();
        }
        assert_eq!(read(&backend, tx), ["alice", "bob"]);
        block_on(backend.tx_ref(tx).insert(
            "notes",
            cells("bob", "attributed"),
            Default::default(),
        ))
        .unwrap();
        commit(&backend, tx, serializable).unwrap();
        let query = backend
            .prepare_query(&Query::from("notes").select(["body", "$createdBy.identity.subject"]))
            .unwrap();
        let rows = block_on(backend.all(&query, ReadOpts::default())).unwrap();
        let attributed = rows
            .iter()
            .find(|r| r.cell("body") == Some(Value::String("attributed".into())))
            .unwrap();
        assert_eq!(
            attributed.cell("$createdBy.identity.subject"),
            Some(Value::Nullable(Some(Box::new(Value::String(
                "alice".into()
            )))))
        );
    }
}

/// Existing unbound transactions retain per-operation session selection for
/// Alice and Bob; introducing bound handles must not change that contract.
#[test]
fn unbound_transactions_keep_per_operation_identity_selection() {
    let db = open(1, AuthorSubject::SYSTEM);
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    for name in ["alice", "bob"] {
        block_on(db.tx_ref(tx).insert(
            "notes",
            cells(name, name),
            InsertOptions {
                identity: WriteIdentity::Session(author(name)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    assert_eq!(read(&db, tx), ["alice", "bob"]);
    commit(&db, tx, false).unwrap();
}

struct RecordTransport {
    inner: Box<dyn Transport>,
    sent: Rc<RefCell<Vec<SyncMessage>>>,
}
impl Transport for RecordTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.sent.borrow_mut().push(message.clone());
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}
fn connect(replica: &Db, server: &Db, identity: AuthorSubject) -> Rc<RefCell<Vec<SyncMessage>>> {
    let (up, down) = duplex();
    let sent = Rc::new(RefCell::new(Vec::new()));
    block_on(replica.connect_upstream(Box::new(RecordTransport {
        inner: up,
        sent: sent.clone(),
    })));
    server.accept_subscriber_with_claims(
        down,
        identity,
        if identity == AuthorSubject::SYSTEM {
            Some(BTreeMap::new())
        } else {
            Some(claims(&identity.principal_parts().1))
        },
    );
    replica.set_remote_link_hint(RemoteLinkHint::Live);
    sent
}
fn drive<T>(replica: &Db, server: &Db, future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        block_on(replica.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    panic!("operation did not settle");
}

/// Alice's delegated remote read is refused before it can use the SYSTEM
/// backend's unscoped coverage. Public transport recording proves no Fetch is
/// sent; a scoped LocalOnly read remains available.
#[test]
fn delegated_remote_reads_fail_before_sending_an_unscoped_fetch() {
    let server = open(1, AuthorSubject::SYSTEM);
    server.set_authority();
    let replica = open(2, AuthorSubject::SYSTEM);
    let sent = connect(&replica, &server, AuthorSubject::SYSTEM);
    let tx = begin(&replica, true, author("alice"));
    let query = replica.prepare_query(&Query::from("notes")).unwrap();
    for tier in [ReadTier::Remote, ReadTier::LocalFirst] {
        let error = block_on(
            replica
                .tx_ref(tx)
                .all_prepared_at(&query, false, None, tier),
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::Query);
        assert!(error.message.contains("scoped coverage"));
    }
    assert!(read(&replica, tx).is_empty());
    assert!(
        !sent
            .borrow()
            .iter()
            .any(|m| matches!(m, SyncMessage::Fetch { .. }))
    );
    replica.abandon_transaction_handle(tx).unwrap();
}

/// Alice's own remote transaction still reads and settles normally. Per-request
/// claim changes cannot silently reuse that connection's remote coverage.
#[test]
fn own_identity_remote_reads_work_but_changed_claims_require_scoped_coverage() {
    let server = open(1, AuthorSubject::SYSTEM);
    server.set_authority();
    seed(&server);
    let replica = open(2, author("alice"));
    connect(&replica, &server, author("alice"));
    let tx = begin(&replica, true, author("alice"));
    let query = replica.prepare_query(&Query::from("notes")).unwrap();
    let rows = drive(
        &replica,
        &server,
        replica
            .tx_ref(tx)
            .all_prepared_at(&query, false, None, ReadTier::Remote),
    )
    .unwrap();
    assert_eq!(bodies(rows), ["alice"]);
    let query = query.with_identity_claims(author("alice"), BTreeMap::new());
    let error = block_on(
        replica
            .tx_ref(tx)
            .all_prepared_at(&query, false, None, ReadTier::Remote),
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::Query);
    let receipt = commit(&replica, tx, true).unwrap();
    drive(
        &replica,
        &server,
        replica.wait_for_transaction(receipt.0, DurabilityTier::Global),
    )
    .unwrap();
}

/// A backend hydrates both accounts, reads only Alice inside a read-only batch,
/// and uploads it. A trusted connection validates Alice's claims; Mallory's
/// ordinary connection cannot vouch for Alice. Public transport recording is
/// needed to check the empty commit's identity, which has no row provenance.
#[test]
fn read_only_commits_preserve_session_identity_and_connection_trust() {
    for trusted in [true, false] {
        let server = open(1, AuthorSubject::SYSTEM);
        server.set_authority();
        seed(&server);
        // Only the trusted backend can vouch for Alice's account claim.
        server.set_identity_claims(author("alice"), BTreeMap::new());
        let replica = open(2, AuthorSubject::SYSTEM);
        let (up, down) = duplex();
        let upstream = block_on(replica.connect_upstream(up));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
        let query = replica.prepare_query(&Query::from("notes")).unwrap();
        let bootstrap = OpenTransactionId::new();
        block_on(replica.begin_serializable(bootstrap)).unwrap();
        let rows = drive(
            &replica,
            &server,
            replica
                .tx_ref(bootstrap)
                .all_prepared_at(&query, false, None, ReadTier::Remote),
        )
        .unwrap();
        assert_eq!(bodies(rows), ["alice", "bob"]);
        replica.abandon_transaction_handle(bootstrap).unwrap();
        assert!(replica.detach_connection(&upstream));
        let sent = connect(
            &replica,
            &server,
            if trusted {
                AuthorSubject::SYSTEM
            } else {
                author("mallory")
            },
        );
        let tx = begin(&replica, true, author("alice"));
        assert_eq!(read(&replica, tx), ["alice"]);
        let receipt = commit(&replica, tx, true).unwrap();
        let result = drive(
            &replica,
            &server,
            replica.wait_for_transaction(receipt.0, DurabilityTier::Global),
        );
        if trusted {
            result.unwrap();
        } else {
            assert!(result.is_err(), "Mallory must not validate Alice's readset");
        }
        let sent = sent.borrow();
        let (commit_author, session) = sent
            .iter()
            .find_map(|message| match message {
                SyncMessage::Commit {
                    author,
                    session,
                    ops,
                    ..
                } if ops.is_empty() => Some((author, session)),
                _ => None,
            })
            .expect("read-only commit was uploaded");
        assert_eq!(commit_author.as_deref(), Some(author("alice").canonical()));
        assert_eq!(session.as_ref(), Some(&claims("alice")));
    }
}
