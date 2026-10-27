//! Initialization reserves an identity while publication keeps normal ordering,
//! policy validation, atomic durable uploads and owner-scoped recovery.
mod common;
use jazz::db::{
    Db, DbConfig, ErrorCode, InitializationTransactionStatus, ReadOpts, ReservedTxId, block_on,
    duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate};
use std::{
    collections::BTreeMap,
    future::Future,
    path::Path,
    pin::pin,
    task::{Context, Poll, Waker},
};
fn author(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}
fn open(node: u8, who: &str, path: Option<&Path>, authority: bool) -> Db {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    let config = DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: author(who),
        },
    );
    // SQLite's public opener admits its own codec; retired Groove storage
    // admission cannot be used for this engine.
    let db = match path {
        Some(p) => block_on(Db::open_at(config, p, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap();
    if authority {
        db.set_authority();
        db.initialize_authority_catalogue().unwrap();
    }
    db
}
fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn cells(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("text".into(), Value::String(text.into()))])
}
fn stage(db: &Db, n: u8) -> OpenTransactionId {
    let id = OpenTransactionId::new();
    block_on(db.begin_serializable(id)).unwrap();
    db.prepare_initialization_insert(id, "notes", row(n))
        .unwrap();
    block_on(db.tx_ref(id).insert(
        "notes",
        cells("founder"),
        jazz::db::InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
    id
}
fn rows(db: &Db) -> Vec<(RowUuid, String)> {
    let q = db.prepare_query(&Query::from("notes")).unwrap();
    let mut rows = block_on(db.all(&q, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.row_uuid(),
                match r.cell_at(0).unwrap() {
                    Value::String(s) => s,
                    _ => panic!("text"),
                },
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|r| r.0);
    rows
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut f = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(v) = f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            return v;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not settle")
}
fn connect(alice: &Db, bob: &Db, who: &str) {
    let (up, down) = duplex();
    block_on(alice.connect_upstream(up));
    bob.accept_subscriber(down, author(who));
}
/// Alice journals a reservation before publication. It is neither a visible
/// row nor a committed status, and sealing retires the old mutable handle.
#[test]
fn seal_is_private_frozen_and_publishes_the_exact_identity() {
    let db = open(1, "alice", None, false);
    let id = stage(&db, 1);
    let seal = db.seal_initialization_transaction(id).unwrap();
    let reserved = seal.reservation();
    assert_eq!(ReservedTxId::decode(&reserved.encode()).unwrap(), reserved);
    assert!(rows(&db).is_empty());
    assert_eq!(
        db.initialization_transaction_status(&[reserved]).unwrap(),
        vec![InitializationTransactionStatus::NotObserved]
    );
    assert!(
        block_on(
            db.tx_ref(id)
                .insert("notes", cells("late"), Default::default())
        )
        .is_err()
    );
    assert!(block_on(db.commit_serializable_handle(id)).is_err());
    assert!(block_on(db.begin_serializable(id)).is_err());
    let receipt = db.publish_initialization_transaction(seal).unwrap();
    assert_eq!(
        db.committed_transaction_id(receipt.0).unwrap(),
        reserved.transaction_id()
    );
    assert_ne!(receipt.0, reserved.transaction_id().time.0);
    assert_eq!(rows(&db), vec![(row(1), "founder".into())]);
    assert!(
        matches!(db.initialization_transaction_status(&[reserved]).unwrap()[0],InitializationTransactionStatus::Complete(ref state) if state.fate==Fate::Pending && state.durability==DurabilityTier::Local)
    );
}
/// Alice writes while her initialization seal is journalled. A snapshot taken
/// in that interval excludes the unpublished seal even after it is published.
#[test]
fn interleaved_writes_and_snapshots_use_publication_order() {
    let db = open(1, "alice", None, false);
    let seal = db.seal_initialization_transaction(stage(&db, 1)).unwrap();
    block_on(db.insert(
        "notes",
        cells("between"),
        jazz::db::InsertOptions {
            row_id: Some(row(2)),
            ..Default::default()
        },
    ))
    .unwrap();
    let snapshot = block_on(db.capture_snapshot()).unwrap();
    let before = OpenTransactionId::new();
    block_on(db.begin_serializable(before)).unwrap();
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        block_on(db.tx_ref(before).all_prepared(&query))
            .unwrap()
            .len(),
        1
    );
    let published = db.publish_initialization_transaction(seal).unwrap();
    assert_eq!(rows(&db).len(), 2);
    assert_eq!(
        block_on(db.tx_ref(before).all_prepared(&query))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(snapshot.dots.len(), 1);
    assert!(published.0 > snapshot.dots[0].time.0);
    db.abandon_transaction_handle(before).unwrap();
}
/// A competing Alice insert can happen after sealing, and normal serializable
/// validation rejects publication. The consumed seal cannot reopen its batch.
#[test]
fn conflict_after_seal_closes_without_publishing() {
    let db = open(1, "alice", None, false);
    let id = stage(&db, 1);
    let seal = db.seal_initialization_transaction(id).unwrap();
    let reserved = seal.reservation();
    block_on(db.insert(
        "notes",
        cells("winner"),
        jazz::db::InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(
        db.publish_initialization_transaction(seal)
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
    assert_eq!(rows(&db), vec![(row(1), "winner".into())]);
    assert_eq!(
        db.initialization_transaction_status(&[reserved]).unwrap(),
        vec![InitializationTransactionStatus::NotObserved]
    );
    assert!(block_on(db.begin_serializable(id)).is_err());
}
/// Cancellation, drop and a foreign runtime all consume the capability without
/// publishing. A cancelled reservation is not reused after SQLite restart.
#[test]
fn cancellation_drop_and_foreign_owner_never_reuse_reservations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let first;
    {
        let db = open(1, "alice", Some(&path), false);
        let seal = db.seal_initialization_transaction(stage(&db, 1)).unwrap();
        first = seal.reservation();
        db.cancel_initialization_transaction(seal).unwrap();
        let seal = db.seal_initialization_transaction(stage(&db, 2)).unwrap();
        let other = open(2, "alice", None, false);
        assert!(other.publish_initialization_transaction(seal).is_err());
        let seal = db.seal_initialization_transaction(stage(&db, 3)).unwrap();
        drop(seal);
        assert!(rows(&db).is_empty());
    }
    let db = open(1, "alice", Some(&path), false);
    let seal = db.seal_initialization_transaction(stage(&db, 4)).unwrap();
    assert_ne!(first, seal.reservation());
}
/// Bob accepts Alice's reserved identity through normal serializable uploads;
/// subsequent transactions can depend on that publication before it settles.
#[test]
fn reserved_upload_and_dependent_transaction_reach_authority() {
    let alice = open(1, "alice", None, false);
    let bob = open(3, "bob", None, true);
    let seal = alice
        .seal_initialization_transaction(stage(&alice, 1))
        .unwrap();
    let reserved = seal.reservation();
    let first = alice.publish_initialization_transaction(seal).unwrap();
    let id = OpenTransactionId::new();
    block_on(alice.begin_serializable(id)).unwrap();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        block_on(alice.tx_ref(id).all_prepared(&query))
            .unwrap()
            .len(),
        1
    );
    block_on(
        alice
            .tx_ref(id)
            .update("notes", row(1), cells("dependent"), Default::default()),
    )
    .unwrap();
    let second = block_on(alice.commit_serializable_handle(id)).unwrap();
    connect(&alice, &bob, "alice");
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(second.0, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(rows(&bob), vec![(row(1), "dependent".into())]);
    assert_eq!(
        alice.committed_transaction_id(first.0).unwrap(),
        reserved.transaction_id()
    );
    assert!(
        matches!(alice.initialization_transaction_status(&[reserved]).unwrap()[0],InitializationTransactionStatus::Complete(ref state) if state.fate==Fate::Accepted && state.durability==DurabilityTier::Global)
    );
}
/// Alice's durable outbox replays publication order, not reservation order, and
/// status lookup remains bounded and bound to the original admitted author.
#[test]
fn pending_publications_survive_reopen_with_exact_origins_and_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let reserved;
    {
        let alice = open(1, "alice", Some(&path), false);
        let seal = alice
            .seal_initialization_transaction(stage(&alice, 1))
            .unwrap();
        reserved = seal.reservation();
        block_on(alice.insert(
            "notes",
            cells("between"),
            jazz::db::InsertOptions {
                row_id: Some(row(2)),
                ..Default::default()
            },
        ))
        .unwrap();
        alice.publish_initialization_transaction(seal).unwrap();
    }
    let alice = open(2, "alice", Some(&path), false);
    assert!(matches!(
        alice
            .initialization_transaction_status(&[reserved])
            .unwrap()[0],
        InitializationTransactionStatus::Complete(_)
    ));
    assert!(
        alice
            .initialization_transaction_status(&[reserved; 65])
            .is_err()
    );
    let bob = open(3, "bob", None, true);
    connect(&alice, &bob, "alice");
    drive(
        &[&alice, &bob],
        alice.wait_for_pending_writes(DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(rows(&bob).len(), 2);
    assert!(
        matches!(alice.initialization_transaction_status(&[reserved]).unwrap()[0],InitializationTransactionStatus::Complete(ref state) if state.fate==Fate::Accepted)
    );
    drop(alice);
    let mallory = open(4, "mallory", Some(&path), false);
    assert_eq!(
        mallory
            .initialization_transaction_status(&[reserved])
            .unwrap(),
        vec![InitializationTransactionStatus::NotObserved]
    );
}

/// Alice's restarted owner refuses an incomplete initialization before sending
/// even an unrelated earlier commit. Physical corruption needs direct SQLite
/// injection: the public API never constructs incomplete publication units.
#[test]
fn incomplete_recovery_blocks_every_upload_prefix() {
    use jazz::db::SyncMessage;
    for damage in [
        "missing-unit",
        "missing-proof",
        "missing-row-manifest",
        "missing-row",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.sqlite");
        let reserved;
        {
            let alice = open(1, "alice", Some(&path), false);
            let seal = alice
                .seal_initialization_transaction(stage(&alice, 1))
                .unwrap();
            reserved = seal.reservation();
            block_on(alice.insert(
                "notes",
                cells("earlier"),
                jazz::db::InsertOptions {
                    row_id: Some(row(2)),
                    ..Default::default()
                },
            ))
            .unwrap();
            alice.publish_initialization_transaction(seal).unwrap();
        }
        {
            let storage = rusqlite::Connection::open(&path).unwrap();
            let tx = reserved.transaction_id().time.0 as i64;
            match damage {
                "missing-unit" => {
                    storage
                        .execute("DELETE FROM _jazz_pending WHERE tx=?1", [tx])
                        .unwrap();
                }
                "missing-proof" => {
                    let bytes: Vec<u8> = storage
                        .query_row(
                            "SELECT commit_msg FROM _jazz_pending WHERE tx=?1",
                            [tx],
                            |r| r.get(0),
                        )
                        .unwrap();
                    let mut message: SyncMessage = postcard::from_bytes(&bytes).unwrap();
                    if let SyncMessage::Commit { kind, .. } = &mut message {
                        *kind = serde_json::from_str("\"Mergeable\"").unwrap();
                    }
                    storage
                        .execute(
                            "UPDATE _jazz_pending SET commit_msg=?1 WHERE tx=?2",
                            rusqlite::params![postcard::to_allocvec(&message).unwrap(), tx],
                        )
                        .unwrap();
                }
                "missing-row-manifest" => {
                    storage
                        .execute("UPDATE _jazz_pending SET rows=x'00' WHERE tx=?1", [tx])
                        .unwrap();
                }
                "missing-row" => {
                    let bytes: Vec<u8> = storage
                        .query_row("SELECT rows FROM _jazz_pending WHERE tx=?1", [tx], |r| {
                            r.get(0)
                        })
                        .unwrap();
                    let keys: Vec<(String, [u8; 16], Vec<u8>)> =
                        postcard::from_bytes(&bytes).unwrap();
                    let table = keys[0].0.replace('"', "\"\"");
                    storage
                        .execute(
                            &format!("DELETE FROM \"{table}\" WHERE id=?1"),
                            [row(1).as_bytes().as_slice()],
                        )
                        .unwrap();
                }
                _ => unreachable!(),
            }
        }
        let alice = open(2, "alice", Some(&path), false);
        assert_eq!(
            alice
                .initialization_transaction_status(&[reserved])
                .unwrap(),
            vec![InitializationTransactionStatus::Incomplete],
            "{damage}"
        );
        let bob = open(3, "bob", None, true);
        connect(&alice, &bob, "alice");
        assert_eq!(
            block_on(alice.tick()).unwrap_err().code,
            ErrorCode::Storage,
            "{damage}"
        );
        block_on(bob.tick()).unwrap();
        assert!(rows(&bob).is_empty(), "{damage}");
    }
}
/// Mallory cannot recover Alice's pending initialization as Mallory's upload.
/// The real owner may reopen the same store and settle the original unit.
#[test]
fn foreign_pending_owner_cannot_replay_initialization() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let reserved;
    {
        let alice = open(1, "alice", Some(&path), false);
        let seal = alice
            .seal_initialization_transaction(stage(&alice, 1))
            .unwrap();
        reserved = seal.reservation();
        alice.publish_initialization_transaction(seal).unwrap();
    }
    let bob = open(3, "bob", None, true);
    {
        let mallory = open(2, "mallory", Some(&path), false);
        assert_eq!(
            mallory
                .initialization_transaction_status(&[reserved])
                .unwrap(),
            vec![InitializationTransactionStatus::NotObserved]
        );
        connect(&mallory, &bob, "mallory");
        assert!(block_on(mallory.tick()).is_err());
        block_on(bob.tick()).unwrap();
        assert!(rows(&bob).is_empty());
    }
    let alice = open(4, "alice", Some(&path), false);
    connect(&alice, &bob, "alice");
    drive(
        &[&alice, &bob],
        alice.wait_for_pending_writes(DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(rows(&bob).len(), 1);
}
/// Alice creates a dependent transaction after her writer lease rotates, before
/// Bob sees the original initialization. Wire dependencies preserve both nodes.
#[test]
fn new_lease_transaction_depends_on_original_initialization() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    {
        let alice = open(1, "alice", Some(&path), false);
        let seal = alice
            .seal_initialization_transaction(stage(&alice, 1))
            .unwrap();
        alice.publish_initialization_transaction(seal).unwrap();
    }
    let alice = open(2, "alice", Some(&path), false);
    let id = OpenTransactionId::new();
    block_on(alice.begin_serializable(id)).unwrap();
    let q = alice.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        block_on(alice.tx_ref(id).all_prepared(&q)).unwrap().len(),
        1
    );
    block_on(
        alice
            .tx_ref(id)
            .update("notes", row(1), cells("after restart"), Default::default()),
    )
    .unwrap();
    let receipt = block_on(alice.commit_serializable_handle(id)).unwrap();
    let bob = open(3, "bob", None, true);
    connect(&alice, &bob, "alice");
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(rows(&bob), vec![(row(1), "after restart".into())]);
}
/// Alice can publish two seals in reverse reservation order; Bob receives the
/// original publication order, even after an owner restart.
#[test]
fn reverse_reservation_order_replays_publication_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    {
        let alice = open(1, "alice", Some(&path), false);
        let first = alice
            .seal_initialization_transaction(stage(&alice, 1))
            .unwrap();
        let second = alice
            .seal_initialization_transaction(stage(&alice, 2))
            .unwrap();
        alice.publish_initialization_transaction(second).unwrap();
        alice.publish_initialization_transaction(first).unwrap();
    }
    let alice = open(2, "alice", Some(&path), false);
    let bob = open(3, "bob", None, true);
    connect(&alice, &bob, "alice");
    drive(
        &[&alice, &bob],
        alice.wait_for_pending_writes(DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(rows(&bob).len(), 2);
}
/// Alice's provisional absence proof rejects an existing, deleted or already
/// staged coordinate, while an ordinary serializable transaction remains usable.
#[test]
fn absence_rejects_deleted_and_staged_coordinates() {
    let alice = open(1, "alice", None, false);
    let id = stage(&alice, 1);
    assert_eq!(
        alice
            .prepare_initialization_insert(id, "notes", row(1))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
    let seal = alice.seal_initialization_transaction(id).unwrap();
    alice.publish_initialization_transaction(seal).unwrap();
    block_on(alice.delete("notes", row(1), Default::default())).unwrap();
    let next = OpenTransactionId::new();
    block_on(alice.begin_serializable(next)).unwrap();
    assert_eq!(
        alice
            .prepare_initialization_insert(next, "notes", row(1))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
}

/// Alice gets no rows, committed status or upload after a storage failure while
/// publishing. A SQLite trigger injects a failure the public API cannot create.
#[test]
fn failed_outbox_persistence_rolls_back_publication() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let alice = open(1, "alice", Some(&path), false);
    let seal = alice
        .seal_initialization_transaction(stage(&alice, 1))
        .unwrap();
    let reserved = seal.reservation();
    let storage = rusqlite::Connection::open(&path).unwrap();
    storage.execute_batch("CREATE TRIGGER fail_initialization BEFORE INSERT ON _jazz_pending BEGIN SELECT RAISE(ABORT,'injected outbox failure'); END;").unwrap();
    assert_eq!(
        alice
            .publish_initialization_transaction(seal)
            .unwrap_err()
            .code,
        ErrorCode::Storage
    );
    assert!(rows(&alice).is_empty());
    assert_eq!(
        alice
            .initialization_transaction_status(&[reserved])
            .unwrap(),
        vec![InitializationTransactionStatus::NotObserved]
    );
    storage
        .execute_batch("DROP TRIGGER fail_initialization")
        .unwrap();
    let next = alice
        .seal_initialization_transaction(stage(&alice, 2))
        .unwrap();
    assert_ne!(next.reservation(), reserved);
    alice.publish_initialization_transaction(next).unwrap();
    let bob = open(3, "bob", None, true);
    connect(&alice, &bob, "alice");
    drive(
        &[&alice, &bob],
        alice.wait_for_pending_writes(DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(rows(&bob), vec![(row(2), "founder".into())]);
}
/// The owner seal cannot authorize Mallory's session or backend attribution,
/// and mergeable batches cannot be relabelled as serializable initialization.
#[test]
fn seal_requires_serializable_owner_identity() {
    let alice = open(1, "alice", None, false);
    for kind in ["mergeable", "foreign-session"] {
        let id = OpenTransactionId::new();
        match kind {
            "mergeable" => block_on(alice.begin_mergeable(id)).unwrap(),
            "foreign-session" => {
                block_on(alice.begin_serializable_for_identity(id, author("mallory"))).unwrap()
            }
            _ => unreachable!(),
        }
        assert!(alice.seal_initialization_transaction(id).is_err(), "{kind}");
        alice.abandon_transaction_handle(id).unwrap();
    }
    let system = open(2, "system", None, false);
    let id = OpenTransactionId::new();
    assert!(block_on(system.begin_serializable_for_attribution(id, author("alice"))).is_err());
}
/// Alice cannot turn a policy-hidden stored coordinate into an absence proof.
/// The existence test must inspect the exact physical id before the read policy.
#[test]
fn absence_does_not_ignore_policy_hidden_rows() {
    use jazz::tools::{PolicyExpr, TablePolicies};
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::False)
                            .with_insert(PolicyExpr::True),
                    ),
            )
            .build(),
    );
    let alice = block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: author("alice"),
        },
    )))
    .unwrap();
    alice.set_remote_link_hint(jazz::db::RemoteLinkHint::NoServer);
    block_on(alice.insert(
        "notes",
        cells("hidden"),
        jazz::db::InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    assert!(
        block_on(alice.all_for_identity(&query, ReadOpts::default(), author("alice")))
            .unwrap()
            .is_empty()
    );
    let id = OpenTransactionId::new();
    block_on(alice.begin_serializable(id)).unwrap();
    assert_eq!(
        alice
            .prepare_initialization_insert(id, "notes", row(1))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
}
/// The shared host registry consumes poisoned transactions instead of sealing
/// only the successful prefix; duplicate staging has normal refusal semantics.
#[test]
fn host_refused_staging_cannot_seal_a_partial_transaction() {
    let alice = open(1, "alice", None, false);
    let identity = jazz::db::DbIdentity {
        node: NodeUuid::from_bytes([1; 16]),
        author: author("alice"),
    };
    let binding = jazz::binding_host::Binding::new(alice, identity, false);
    let id = stage(&binding.db, 1).to_string();
    let descriptor =
        jazz::records::RecordDescriptor::new([("text", jazz::records::ValueType::String)]);
    let record = descriptor
        .create(&[Value::String("duplicate".into())])
        .unwrap();
    let bytes = jazz::binding_codec::encode_named_cells(&jazz::records::OwnedRecord::new(
        record, descriptor,
    ))
    .unwrap();
    binding
        .insert_in_transaction(
            &id,
            "notes",
            &bytes,
            &serde_json::json!({"rowId":vec![1;16]}),
        )
        .unwrap();
    assert!(binding.seal_initialization_transaction(&id).is_err());
    assert!(rows(&binding.db).is_empty());
}

/// Alice's new wire dependency mapping has a named byte contract. This lower
/// level fixture is necessary because API success alone cannot pin wire bytes.
#[test]
fn initialization_wire_encoding_fixture() {
    let message:jazz::db::SyncMessage=serde_json::from_value(serde_json::json!({"Commit":{
        "node":vec![2;16],"tx":4503599627370499u64,
        "kind":{"SerializableOriginsV1":{"snapshot":7,"reads":[],"own":[[4503599627370498u64,vec![1;16],4503599627370497u64]]}},
        "ops":[],"author":null,"session":null,"expect":[]
    }})).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v2/initialization.json"
    ))
    .unwrap();
    let bytes = jazz::db::encode_message(&message).unwrap();
    assert_eq!(hex::encode(&bytes), fixture["commitHex"].as_str().unwrap());
    let roundtrip = jazz::db::decode_message(&bytes).unwrap();
    assert_eq!(
        serde_json::to_value(&roundtrip).unwrap(),
        serde_json::to_value(&message).unwrap()
    );
}

/// The real stored publication pins SQL types, raw UUID bytes and the pending
/// recovery index. The versioned reservation and SDK status bridge pin bytes.
/// Direct SQL inspection is necessary to verify the durable encoding contract.
#[test]
fn reservation_storage_and_status_bridge_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_initialization_v1.json")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let alice = open(1, "alice", Some(&path), false);
    let identity = jazz::db::DbIdentity {
        node: NodeUuid::from_bytes([1; 16]),
        author: author("alice"),
    };
    let binding = jazz::binding_host::Binding::new(alice, identity, false);
    let id = stage(&binding.db, 1);
    let seal = binding
        .seal_initialization_transaction(&id.to_string())
        .unwrap();
    assert_eq!(
        seal.reserved_tx_id,
        fixture["reservation"].as_str().unwrap()
    );
    assert_eq!(
        hex::encode(seal.reserved_tx_id.as_bytes()),
        fixture["reservationHex"].as_str().unwrap()
    );
    binding
        .publish_initialization_transaction(&seal.token)
        .unwrap();
    assert_eq!(
        binding
            .initialization_transaction_status(&[seal.reserved_tx_id])
            .unwrap(),
        fixture["pendingStatusJson"].as_str().unwrap()
    );
    let storage = rusqlite::Connection::open(&path).unwrap();
    let record=storage.query_row("SELECT seq,hex(node),tx,author,initialization,pending,typeof(node),typeof(tx) FROM _jazz_local_commits_v1",[],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,String>(3)?,r.get::<_,i64>(4)?,r.get::<_,i64>(5)?,r.get::<_,String>(6)?,r.get::<_,String>(7)?))).unwrap();
    let expected = &fixture["localCommit"];
    assert_eq!(record.0, expected["seq"].as_i64().unwrap());
    assert_eq!(record.1, expected["nodeHex"].as_str().unwrap());
    assert_eq!(record.2, expected["tx"].as_i64().unwrap());
    assert_eq!(record.3, author("alice").canonical());
    assert_eq!((record.4, record.5), (1, 1));
    assert_eq!((record.6.as_str(), record.7.as_str()), ("blob", "integer"));
    let plan:String=storage.query_row("EXPLAIN QUERY PLAN SELECT seq,author FROM _jazz_local_commits_v1 WHERE initialization=1 AND pending=1 ORDER BY seq",[],|r|r.get(3)).unwrap();
    assert!(plan.contains("_jazz_initialization_pending_v1"), "{plan}");
    for invalid in [
        "jazz-init-v1:01010101-0101-0101-0101-010101010101:1:0",
        "jazz-sqlite-init-v2:01010101-0101-0101-0101-010101010101:1",
        "jazz-sqlite-init-v1:01010101010101010101010101010101:1",
        "jazz-sqlite-init-v1:01010101-0101-0101-0101-010101010101:01",
        "jazz-sqlite-init-v1:01010101-0101-0101-0101-010101010101:0",
        "jazz-sqlite-init-v1:01010101-0101-0101-0101-010101010101:9223372036854775807",
        "jazz-sqlite-init-v1:01010101-0101-0101-0101-010101010101:1:0",
    ] {
        assert!(ReservedTxId::decode(invalid).is_err(), "{invalid}");
    }
}

struct TamperOrigins {
    transport: Box<dyn jazz::db::Transport>,
    mode: &'static str,
}
impl jazz::db::Transport for TamperOrigins {
    fn send(&mut self, message: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
        let mut value = serde_json::to_value(&message).unwrap();
        if value["Commit"]["kind"]
            .get("SerializableOriginsV1")
            .is_some()
        {
            match self.mode {
                "duplicate" => {
                    let own = value["Commit"]["kind"]["SerializableOriginsV1"]["own"]
                        .as_array_mut()
                        .unwrap();
                    own.push(own[0].clone());
                }
                "invalid-position" => {
                    value["Commit"]["kind"]["SerializableOriginsV1"]["own"][0][0] = 1.into()
                }
                "forged-author" => {
                    value["Commit"]["author"] = author("mallory").canonical().into();
                    value["Commit"]["session"] = serde_json::json!({});
                }
                _ => unreachable!(),
            }
        }
        self.transport.send(serde_json::from_value(value).unwrap())
    }
    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        self.transport.try_recv()
    }
}
/// Bob rejects malformed origin mappings. A changed author on Alice's ordinary
/// session link is normalized to Alice, including the persisted row provenance.
#[test]
fn origin_dependencies_still_validate_shape_and_authenticated_scope() {
    for mode in ["duplicate", "invalid-position", "forged-author"] {
        let alice = open(1, "alice", None, false);
        let bob = open(3, "bob", None, true);
        let seal = alice
            .seal_initialization_transaction(stage(&alice, 1))
            .unwrap();
        alice.publish_initialization_transaction(seal).unwrap();
        let id = OpenTransactionId::new();
        block_on(alice.begin_serializable(id)).unwrap();
        block_on(
            alice
                .tx_ref(id)
                .update("notes", row(1), cells("follow-up"), Default::default()),
        )
        .unwrap();
        let receipt = block_on(alice.commit_serializable_handle(id)).unwrap();
        let (up, down) = duplex();
        block_on(alice.connect_upstream(Box::new(TamperOrigins {
            transport: up,
            mode,
        })));
        bob.accept_subscriber(down, author("alice"));
        let result = drive(
            &[&alice, &bob],
            alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
        );
        if mode == "forged-author" {
            result.unwrap();
            assert_eq!(rows(&bob), vec![(row(1), "follow-up".into())]);
            let provenance = bob.row_provenance("notes", row(1)).unwrap().unwrap();
            assert_eq!(provenance.1, author("alice").canonical());
            assert_eq!(provenance.3, author("alice").canonical());
        } else {
            assert!(result.is_err(), "{mode}");
            assert_eq!(rows(&bob), vec![(row(1), "founder".into())], "{mode}");
        }
    }
}
/// Bob's authority-local initialization reports the reserved transaction in
/// accepted history. Alice's unaccepted initialization is never such evidence.
#[test]
fn accepted_history_uses_reserved_identity_without_exposing_pending_rows() {
    use jazz::query::{col, eq, lit};
    for authority in [false, true] {
        let db = open(1, "alice", None, authority);
        let seal = db.seal_initialization_transaction(stage(&db, 1)).unwrap();
        let reserved = seal.reservation();
        db.publish_initialization_transaction(seal).unwrap();
        let q = db
            .prepare_query(
                &Query::from("history.notes").filter(eq(col("id"), lit(Value::Uuid(row(1).0)))),
            )
            .unwrap();
        let history = block_on(db.all(&q, ReadOpts::default())).unwrap();
        if authority {
            assert_eq!(history.len(), 1);
            let cell = history[0]
                .cells()
                .find(|(name, _)| *name == "$transactionId")
                .unwrap()
                .1
                .cloned()
                .unwrap();
            assert_eq!(
                cell,
                Value::Tuple(vec![
                    Value::U64(reserved.transaction_id().time.0),
                    Value::Uuid(reserved.transaction_id().node.0)
                ])
            );
        } else {
            assert!(history.is_empty());
        }
    }
}

/// Alice can create a write-only founder but cannot update it. Bob's first
/// receipt must preserve the founder image, not Alice's later rejected edit.
#[test]
fn write_only_ack_preserves_exact_initialization_image() {
    use jazz::tools::{PolicyExpr, TablePolicies};
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::False)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::False),
                    ),
            )
            .build(),
    );
    let alice = block_on(Db::open(DbConfig::new(
        schema.clone(),
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: author("alice"),
        },
    )))
    .unwrap();
    let bob = block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([3; 16]),
            author: author("bob"),
        },
    )))
    .unwrap();
    bob.set_authority();
    bob.initialize_authority_catalogue().unwrap();
    let seal = alice
        .seal_initialization_transaction(stage(&alice, 1))
        .unwrap();
    let first = alice.publish_initialization_transaction(seal).unwrap();
    let edit = block_on(alice.update("notes", row(1), cells("rejected edit"), Default::default()))
        .unwrap();
    connect(&alice, &bob, "alice");
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(first.0, DurabilityTier::Global),
    )
    .unwrap();
    assert!(
        drive(
            &[&alice, &bob],
            alice.wait_for_transaction(edit.mergeable_tx_id(), DurabilityTier::Global)
        )
        .is_err()
    );
    assert_eq!(rows(&alice), vec![(row(1), "founder".into())]);
    assert_eq!(rows(&bob), vec![(row(1), "founder".into())]);
}

/// Closing Alice's shared host schema view preserves the root's capability;
/// closing the root retires it across every surviving view.
#[test]
fn shared_host_view_close_preserves_owner_tokens() {
    let db = open(1, "alice", None, false);
    let binding = jazz::binding_host::Binding::new(
        db,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: author("alice"),
        },
        false,
    );
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("text", ColumnType::Text)
                .policies(common::allow_all_policies()),
        )
        .build();
    let view = binding
        .register_schema(&serde_json::to_vec(&schema).unwrap())
        .unwrap();
    let seal = view
        .seal_initialization_transaction(&stage(&view.db, 1).to_string())
        .unwrap();
    block_on(view.close()).unwrap();
    binding
        .publish_initialization_transaction(&seal.token)
        .unwrap();
    assert_eq!(rows(&binding.db).len(), 1);
    let second = binding
        .seal_initialization_transaction(&stage(&binding.db, 2).to_string())
        .unwrap();
    block_on(binding.close()).unwrap();
    assert!(
        view.publish_initialization_transaction(&second.token)
            .is_err()
    );
}
