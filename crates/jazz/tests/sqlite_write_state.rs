//! Write receipts distinguish local persistence from authority settlement, and
//! replicas retain the authority's rejection across SQLite restarts.
mod common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, LOCAL_BASE, RemoteLinkHint, WireError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate, RejectionReason};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

fn author() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn config(node: u8) -> DbConfig {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::eq_literal(
                                "text",
                                jazz::tools::Value::Text("allowed".into()),
                            ))
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    );
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: author(),
        },
    )
}
fn file(path: &Path) -> Db {
    block_on(Db::open_at(config(1), path, Default::default())).unwrap()
}
fn insert(db: &Db, text: &str) -> u64 {
    block_on(db.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String(text.into()))]),
        Default::default(),
    ))
    .unwrap()
    .mergeable_tx_id()
}
fn connect(client: &Db, server: &Db) {
    let (a, b) = duplex_transport::duplex();
    block_on(client.connect_upstream(a));
    server.accept_subscriber(b, author());
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..100 {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("write did not settle");
}
fn count(db: &Db) -> usize {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    block_on(db.all(&query, Default::default())).unwrap().len()
}

/// Alice applies an empty offline update, then restarts. Bob's authority must
/// still receive the original insert exactly once and decide its original fate.
/// The no-op cannot replace that pending unit or manufacture an accepted receipt.
#[test]
fn empty_update_preserves_the_pending_insert_across_reopen() {
    for allowed in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty-update.sqlite");
        let tx = {
            let db = file(&path);
            db.set_remote_link_hint(RemoteLinkHint::Failed);
            let row = block_on(db.insert(
                "notes",
                BTreeMap::from([(
                    "text".into(),
                    Value::String(if allowed { "allowed" } else { "forbidden" }.into()),
                )]),
                Default::default(),
            ))
            .unwrap();
            let tx = row.mergeable_tx_id();
            let no_op =
                block_on(db.update("notes", row.row_uuid(), BTreeMap::new(), Default::default()))
                    .unwrap();
            assert_eq!(no_op.mergeable_tx_id(), tx);
            assert_eq!(db.write_state(tx).unwrap().fate, Fate::Pending);
            assert_eq!(
                db.write_state(tx).unwrap().durability,
                DurabilityTier::Local
            );
            block_on(db.close()).unwrap();
            tx
        };
        let db = file(&path);
        let authority = block_on(Db::open_history_complete(config(2))).unwrap();
        assert_eq!(db.write_state(tx).unwrap().fate, Fate::Pending);
        connect(&db, &authority);
        let outcome = drive(
            &[&db, &authority, &db],
            db.wait_for_transaction(tx, DurabilityTier::Global),
        );
        if allowed {
            outcome.unwrap();
            assert_eq!(db.write_state(tx).unwrap().fate, Fate::Accepted);
            assert_eq!(count(&authority), 1);
        } else {
            assert_eq!(outcome.unwrap_err().code, ErrorCode::WriteRejected);
            assert_eq!(
                db.write_state(tx).unwrap().fate,
                Fate::Rejected(RejectionReason::AuthorizationDenied)
            );
            assert_eq!(count(&authority), 0);
        }
        assert_eq!(count(&db), usize::from(allowed));
    }
}

/// Alice's no-op belongs to its existing mergeable transaction. The authority
/// accepts both inserts together, including the row touched by the empty update.
#[test]
fn empty_update_in_an_open_unit_preserves_all_transaction_writes() {
    let db = block_on(Db::open(config(1))).unwrap();
    db.set_remote_link_hint(RemoteLinkHint::Failed);
    let transaction = block_on(db.mergeable_tx()).unwrap();
    let first = block_on(transaction.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String("allowed".into()))]),
        Default::default(),
    ))
    .unwrap();
    block_on(transaction.update(
        "notes",
        first.row_uuid(),
        BTreeMap::new(),
        Default::default(),
    ))
    .unwrap();
    block_on(transaction.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String("allowed".into()))]),
        Default::default(),
    ))
    .unwrap();
    let tx = block_on(transaction.commit()).unwrap().0;
    let authority = block_on(Db::open_history_complete(config(2))).unwrap();
    connect(&db, &authority);
    drive(
        &[&db, &authority, &db],
        db.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(db.write_state(tx).unwrap().fate, Fate::Accepted);
    assert_eq!(count(&authority), 2);
    assert_eq!(count(&db), 2);
}

/// A standalone local write has no authority receipt; only an explicit
/// authority may report its own committed sequence as globally accepted.
#[test]
fn local_persistence_does_not_invent_authority_acceptance() {
    let db = block_on(Db::open(config(1))).unwrap();
    let tx = insert(&db, "allowed");
    let state = db.write_state(tx).unwrap();
    assert_eq!(state.fate, Fate::Pending);
    assert_eq!(state.durability, DurabilityTier::Local);
    assert_eq!(state.global_time, None);
    assert_eq!(
        block_on(db.wait_for_transaction(tx, DurabilityTier::Global))
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
    assert_eq!(
        db.write_state(tx + 1).unwrap_err().code,
        ErrorCode::NotObserved
    );
    let authority = block_on(Db::open_history_complete(config(2))).unwrap();
    let tx = insert(&authority, "allowed");
    let state = authority.write_state(tx).unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert_eq!(state.durability, DurabilityTier::Global);
    assert_eq!(state.global_time.unwrap().0, tx);
}

/// Alice's offline write survives a restart as pending, then retains the
/// authority's exact accepted sequence after a second restart without a link.
#[test]
fn pending_and_accepted_receipts_survive_replica_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.sqlite");
    let tx = {
        let db = file(&path);
        db.set_remote_link_hint(RemoteLinkHint::Failed);
        let tx = insert(&db, "allowed");
        assert!(tx > LOCAL_BASE);
        assert_eq!(db.write_state(tx).unwrap().fate, Fate::Pending);
        tx
    };
    let server = block_on(Db::open_history_complete(config(2))).unwrap();
    let accepted = {
        let db = file(&path);
        assert_eq!(db.write_state(tx).unwrap().fate, Fate::Pending);
        connect(&db, &server);
        drive(
            &[&db, &server, &db],
            db.wait_for_transaction(tx, DurabilityTier::Global),
        )
        .unwrap();
        let state = db.write_state(tx).unwrap();
        assert_eq!(state.fate, Fate::Accepted);
        assert_eq!(state.durability, DurabilityTier::Global);
        assert_eq!(state.global_time.unwrap().0, server.seq_high_water());
        assert!(state.global_time.unwrap().0 < LOCAL_BASE);
        state
    };
    let reopened = file(&path);
    assert_eq!(reopened.write_state(tx).unwrap(), accepted);
    assert_eq!(
        block_on(reopened.wait_for_transaction(tx, DurabilityTier::Global)).unwrap(),
        tx
    );
    assert_eq!(count(&server), 1);
}

/// The authority rejects Alice's optimistic row. Both the structured fate
/// and the exact wait error remain rejected after the replica restarts.
#[test]
fn rejected_receipt_and_reason_survive_replica_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.sqlite");
    let server = block_on(Db::open_history_complete(config(2))).unwrap();
    let (tx, error) = {
        let db = file(&path);
        connect(&db, &server);
        let tx = insert(&db, "forbidden");
        let error = drive(
            &[&db, &server, &db],
            db.wait_for_transaction(tx, DurabilityTier::Global),
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::WriteRejected);
        assert_eq!(
            db.write_state(tx).unwrap().fate,
            Fate::Rejected(RejectionReason::AuthorizationDenied)
        );
        assert_eq!(count(&db), 0);
        (tx, error)
    };
    let db = file(&path);
    let state = db.write_state(tx).unwrap();
    assert_eq!(
        state.fate,
        Fate::Rejected(RejectionReason::AuthorizationDenied)
    );
    assert_eq!(state.durability, DurabilityTier::None);
    assert_eq!(state.global_time, None);
    for tier in [
        DurabilityTier::None,
        DurabilityTier::Local,
        DurabilityTier::Global,
    ] {
        let restored = block_on(db.wait_for_transaction(tx, tier)).unwrap_err();
        assert_eq!(restored.code, error.code);
        assert_eq!(restored.message, error.message);
    }
    assert_eq!(count(&server), 0);
}

/// Alice's ordinary worker previews her queued upload. Only the authority's
/// acceptance can settle the original transaction globally.
#[test]
fn relay_uploads_wait_for_authority_acceptance() {
    let server = block_on(Db::open_history_complete(config(3))).unwrap();
    let relay = block_on(Db::open(config(2))).unwrap();
    let client = block_on(Db::open(config(1))).unwrap();
    connect(&relay, &server);
    connect(&client, &relay);
    let tx = insert(&client, "allowed");
    for _ in 0..4 {
        block_on(client.tick()).unwrap();
        block_on(relay.tick()).unwrap();
    }
    assert_eq!(client.write_state(tx).unwrap().fate, Fate::Pending);
    assert_eq!(
        count(&relay),
        1,
        "the ordinary worker retains Alice's preview"
    );
    assert_eq!(client.write_state(tx).unwrap().global_time, None);
    assert_eq!(count(&server), 0);
    drive(
        &[&client, &relay, &server, &relay, &client],
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    let state = client.write_state(tx).unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert_eq!(state.global_time.unwrap().0, server.seq_high_water());
    assert!(state.global_time.unwrap().0 < LOCAL_BASE);
    assert_eq!(count(&relay), 1);
    assert_eq!(count(&server), 1);
    assert_eq!(count(&client), 1);
}

/// This lower-level fixture deliberately freezes the durable result envelope:
/// authorities already use these bytes, and replicas now persist them too.
#[test]
fn write_fate_v1_keeps_the_existing_postcard_encoding() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_write_fate_v1.json")).unwrap();
    assert_eq!(fixture["role"], "jazz.sqlite.write-fate.v1");
    assert_eq!(fixture["version"], 1);
    let cases = [
        ("accepted", Ok(())),
        ("conflict", Err(WireError::Conflict("no".into()))),
        (
            "rejected",
            Err(WireError::Rejected {
                reason: None,
                message: "no".into(),
            }),
        ),
        ("other", Err(WireError::Other("no".into()))),
    ];
    for (name, fate) in cases {
        let bytes = postcard::to_allocvec(&fate).unwrap();
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(hex, fixture[name].as_str().unwrap());
        let decoded: Result<(), WireError> = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(postcard::to_allocvec(&decoded).unwrap(), bytes);
    }
}

/// Direct SQL corruption is intentional: the public API must surface a storage
/// error instead of interpreting an undecodable recorded refusal as success.
#[test]
fn corrupt_persisted_fate_never_becomes_success() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.sqlite");
    let tx = {
        let db = file(&path);
        insert(&db, "allowed")
    };
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute(
        "INSERT INTO _jazz_applied (node, tx, result) VALUES (?1, ?2, ?3)",
        rusqlite::params![vec![1u8; 16], tx as i64, vec![255u8]],
    )
    .unwrap();
    drop(sql);
    let db = file(&path);
    assert_eq!(db.write_state(tx).unwrap_err().code, ErrorCode::Storage);
    assert_eq!(
        block_on(db.wait_for_transaction(tx, DurabilityTier::Global))
            .unwrap_err()
            .code,
        ErrorCode::Storage
    );
}

/// Inject the two incomplete receipt forms produced by earlier experimental
/// runtimes: a rejection lacking its error and an optimistic relay acceptance.
/// Neither is enough evidence for a successful global wait or state snapshot.
#[test]
fn incomplete_or_optimistic_receipts_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.sqlite");
    let (rejected, optimistic) = {
        let db = file(&path);
        (insert(&db, "allowed"), insert(&db, "allowed"))
    };
    let sql = rusqlite::Connection::open(&path).unwrap();
    for (tx, seq) in [(rejected, None), (optimistic, Some(LOCAL_BASE + 1))] {
        sql.execute(
            "INSERT INTO _jazz_settled (node, tx, seq) VALUES (?1, ?2, ?3)",
            rusqlite::params![vec![1u8; 16], tx as i64, seq.map(|s| s as i64)],
        )
        .unwrap();
    }
    drop(sql);
    let db = file(&path);
    for tx in [rejected, optimistic] {
        assert_eq!(db.write_state(tx).unwrap_err().code, ErrorCode::NotObserved);
        assert_eq!(
            block_on(db.wait_for_transaction(tx, DurabilityTier::Global))
                .unwrap_err()
                .code,
            ErrorCode::NotObserved
        );
    }
}
