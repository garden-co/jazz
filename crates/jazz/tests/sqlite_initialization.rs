//! Initialization reserves an identity while publication keeps normal ordering,
//! policy validation, atomic durable uploads and owner-scoped recovery.
use super::common;
use jazz::db::InitializationFate as Fate;
use jazz::db::{
    Db, DbConfig, ErrorCode, InitializationTransactionStatus, ReadOpts, ReservedTxId, block_on,
    duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
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
            // Current multi-user authority host; subscribers below remain real
            // authenticated Alice sessions and retain all policy checks.
            author: if authority {
                AuthorSubject::SYSTEM
            } else {
                author(who)
            },
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
    }
    if !authority {
        db.enable_initialization_journal().unwrap();
    }
    db
}
// This is a real authenticated physical authority. It is never constructed
// by converting a replica or by granting a SYSTEM client a seal.
fn physical_authority(node: u8, who: &str, path: Option<&Path>) -> Db {
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
    let db = match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap();
    db.set_authority();
    db.enable_initialization_journal().unwrap();
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
fn trace_enabled() -> bool {
    std::env::var("JAZZ_E14_WRITE_ONLY_TRACE").as_deref() == Ok("actual-two-transactions")
}
fn trace_state(point: &str, index: usize, db: &Db) {
    if !trace_enabled() {
        return;
    }
    let state = db.sync_debug();
    let pending: Vec<u64> = state
        .strip_prefix("pending ")
        .and_then(|value| value.find(']').map(|end| &value[..=end]))
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or_default();
    let origins=pending.into_iter().map(|position|match db.committed_transaction_id(position) {Ok(id)=>serde_json::json!({"localPosition":position,"originalNode":id.node.0.to_string(),"originalTx":id.time.0}),Err(error)=>serde_json::json!({"localPosition":position,"error":error.code})}).collect::<Vec<_>>();
    eprintln!(
        "INIT_TRACE_STATE {}",
        serde_json::json!({"point":point,"ownerIndex":index,"actualAuthority":db.is_authority(),"state":state,"origins":origins})
    );
}
struct PassiveTraceTransport {
    transport: Box<dyn jazz::db::Transport>,
    direction: &'static str,
}
impl jazz::db::Transport for PassiveTraceTransport {
    fn send(&mut self, message: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
        eprintln!(
            "INIT_TRACE_MESSAGE {}",
            serde_json::json!({"direction":self.direction,"actualStructuredTransportMessage":&message})
        );
        self.transport.send(message)
    }
    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        self.transport.try_recv()
    }
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut f = pin!(future);
    for _ in 0..100 {
        for (index, db) in dbs.iter().enumerate() {
            trace_state("before-future-poll", index, db);
        }
        if let Poll::Ready(v) = f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            return v;
        }
        for (index, db) in dbs.iter().enumerate() {
            block_on(db.tick()).unwrap();
            trace_state("after-actual-tick", index, db);
        }
    }
    panic!("did not settle")
}
fn connect(alice: &Db, bob: &Db, who: &str) {
    let (up, down) = duplex();
    let (up, down) = if trace_enabled() {
        (
            Box::new(PassiveTraceTransport {
                transport: up,
                direction: "Alice-to-authority",
            }) as Box<dyn jazz::db::Transport>,
            Box::new(PassiveTraceTransport {
                transport: down,
                direction: "authority-to-Alice",
            }) as Box<dyn jazz::db::Transport>,
        )
    } else {
        (up, down)
    };
    block_on(alice.connect_upstream(up));
    bob.accept_subscriber(down, author(who)).unwrap();
    if trace_enabled() {
        eprintln!(
            "INIT_TRACE_ADMISSION {}",
            serde_json::json!({"actualAcceptedSubscriber":author(who).canonical(),"plainTextFixture":true,"keyOrCipherSetup":false})
        );
    }
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
            // Public reservation ID is intentionally not the local pending key.
            let tx:i64 = storage.query_row("SELECT position FROM _jazz_initialization_publications_v1 WHERE node=?1 AND tx=?2",rusqlite::params![reserved.transaction_id().node.as_bytes().as_slice(),reserved.transaction_id().time.0 as i64],|row|row.get(0)).unwrap();
            match damage {
                "missing-unit" => {
                    storage
                        .execute("DELETE FROM _jazz_pending WHERE tx=?1", [tx])
                        .unwrap();
                }
                "missing-proof" => {
                    let bytes: Vec<u8> = storage
                        .query_row("SELECT header FROM _jazz_pending WHERE tx=?1", [tx], |r| {
                            r.get(0)
                        })
                        .unwrap();
                    let mut message: SyncMessage = postcard::from_bytes(&bytes).unwrap();
                    if let SyncMessage::Commit { kind, .. } = &mut message {
                        *kind = serde_json::from_str("\"Mergeable\"").unwrap();
                    }
                    storage
                        .execute(
                            "UPDATE _jazz_pending SET header=?1 WHERE tx=?2",
                            rusqlite::params![postcard::to_allocvec(&message).unwrap(), tx],
                        )
                        .unwrap();
                }
                "missing-row-manifest" => {
                    storage
                        .execute("DELETE FROM _jazz_pending_rows WHERE tx=?1", [tx])
                        .unwrap();
                }
                "missing-row" => {
                    let table: String = storage
                        .query_row(
                            "SELECT tbl FROM _jazz_pending_rows WHERE tx=?1 ORDER BY n LIMIT 1",
                            [tx],
                            |row| row.get(0),
                        )
                        .unwrap();
                    let table = table.replace('"', "\"\"");
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

/// Independently assembled current bytes pin both unchanged legacy cases and
/// the appended origin case used by current wire messages and pending headers.
#[test]
fn initialization_wire_encoding_fixture() {
    use jazz::db::{Acting, SyncMessage, WireKind, decode_message, encode_frames};
    use jazz::time::TxTime;
    use jazz::tx::TxId;
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite_initialization_current_v1.json"
    ))
    .unwrap();
    for (field, kind) in [
        ("legacy_mergeable_hex", WireKind::Mergeable),
        (
            "legacy_serializable_hex",
            WireKind::Serializable {
                snapshot: 7,
                reads: vec![],
                own: vec![12],
            },
        ),
        (
            "origins_v2_hex",
            WireKind::SerializableOriginsV2 {
                snapshot: 7,
                reads: vec![],
                own: vec![(12, [1; 16], 3)],
            },
        ),
    ] {
        let message = SyncMessage::Commit {
            id: TxId::new(TxTime(9), NodeUuid::from_bytes([2; 16])),
            clock: 11,
            kind,
            ops: vec![],
            acting: Acting::Own,
        };
        let expected = hex::decode(fixture[field].as_str().unwrap()).unwrap();
        assert_eq!(
            postcard::to_allocvec(&message).unwrap(),
            expected,
            "{field}"
        );
        assert_eq!(encode_frames(message.clone()).unwrap(), [expected.clone()]);
        assert_eq!(
            serde_json::to_value(decode_message(&expected).unwrap()).unwrap(),
            serde_json::to_value(&message).unwrap()
        );
    }
    // Exact pre-extension enum subset. This is a byte-level old-reader refusal
    // control, not a claim that an old runtime was executed or can downgrade.
    #[allow(dead_code)]
    #[derive(serde::Deserialize)]
    enum LegacyKind {
        Mergeable,
        Serializable {
            snapshot: u64,
            reads: Vec<()>,
            own: Vec<u64>,
        },
    }
    #[allow(dead_code)]
    #[derive(serde::Deserialize)]
    enum LegacyMessage {
        Read,
        Unsubscribe,
        ResendWhole,
        Settled,
        Commit {
            id: TxId,
            clock: u64,
            kind: LegacyKind,
            ops: Vec<()>,
            acting: Acting,
        },
    }
    for field in ["legacy_mergeable_hex", "legacy_serializable_hex"] {
        assert!(
            postcard::from_bytes::<LegacyMessage>(
                &hex::decode(fixture[field].as_str().unwrap()).unwrap()
            )
            .is_ok()
        );
    }
    assert!(
        postcard::from_bytes::<LegacyMessage>(
            &hex::decode(fixture["origins_v2_hex"].as_str().unwrap()).unwrap()
        )
        .is_err()
    );
    for own in [
        vec![(0, [1; 16], 3)],
        vec![(12, [1; 16], 3), (12, [1; 16], 3)],
        vec![(12, [1; 16], i64::MAX as u64)],
    ] {
        let malformed = SyncMessage::Commit {
            id: TxId::new(TxTime(9), NodeUuid::from_bytes([2; 16])),
            clock: 11,
            kind: WireKind::SerializableOriginsV2 {
                snapshot: 7,
                reads: vec![],
                own,
            },
            ops: vec![],
            acting: Acting::Own,
        };
        assert!(decode_message(&postcard::to_allocvec(&malformed).unwrap()).is_err());
    }
}

/// Real current SQL types, raw UUIDs, pending proof digest and status envelope.
/// Retired SQL/wire layouts are not copied into the current engine.
#[test]
fn reservation_storage_and_status_bridge_fixture() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite_initialization_current_v1.json"
    ))
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let binding = jazz::binding_host::Binding::new(
        open(1, "alice", Some(&path), false),
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: author("alice"),
        },
        false,
    );
    let seal = binding
        .seal_initialization_transaction(&stage(&binding.db, 1).to_string())
        .unwrap();
    assert_eq!(
        seal.reserved_tx_id,
        fixture["reserved_tx_id"].as_str().unwrap()
    );
    binding
        .publish_initialization_transaction(&seal.token)
        .unwrap();
    assert!(
        binding
            .publish_initialization_transaction(&seal.token)
            .is_err()
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            &binding
                .initialization_transaction_status(&[seal.reserved_tx_id.clone()])
                .unwrap()
        )
        .unwrap(),
        fixture["pending_status"]
    );
    let storage = rusqlite::Connection::open(&path).unwrap();
    let record=storage.query_row("SELECT position,hex(node),tx,author,initialization,terminal,typeof(node),typeof(tx) FROM _jazz_initialization_publications_v1",[],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,String>(3)?,r.get::<_,i64>(4)?,r.get::<_,i64>(5)?,r.get::<_,String>(6)?,r.get::<_,String>(7)?))).unwrap();
    let expected = &fixture["stored_publication"];
    assert_eq!(record.0, expected["position"].as_i64().unwrap());
    assert_eq!(record.1, expected["node_hex"].as_str().unwrap());
    assert_eq!(record.2, expected["tx"].as_i64().unwrap());
    assert_eq!(record.3, author("alice").canonical());
    assert_eq!((record.4, record.5), (1, 0));
    assert_eq!((record.6.as_str(), record.7.as_str()), ("blob", "integer"));
    let digest: i64 = storage
        .query_row(
            "SELECT length(digest) FROM _jazz_initialization_pending_v1 WHERE tx=?1",
            [record.0],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(digest, 32);
    let plan:String=storage.query_row("EXPLAIN QUERY PLAN SELECT position FROM _jazz_initialization_publications_v1 WHERE terminal=0 AND position IS NOT NULL ORDER BY position",[],|r|r.get(3)).unwrap();
    assert!(
        plan.contains("_jazz_initialization_outstanding_v1"),
        "{plan}"
    );
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
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    bob.set_authority();
    alice.enable_initialization_journal().unwrap();
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

// Control uses the ordinary public replica path, without a reservation,
// initialization journal, prepared slot or sealed transaction.
fn ordinary_write_only_case(serializable: bool) {
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
            node: NodeUuid::from_bytes([11; 16]),
            author: author("alice"),
        },
    )))
    .unwrap();
    alice.become_replica();
    let bob = block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([13; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    bob.set_authority();
    let first = if serializable {
        let id = OpenTransactionId::new();
        block_on(alice.begin_serializable(id)).unwrap();
        block_on(alice.tx_ref(id).insert(
            "notes",
            cells("founder"),
            jazz::db::InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap();
        block_on(alice.commit_serializable_handle(id)).unwrap().0
    } else {
        block_on(alice.insert(
            "notes",
            cells("founder"),
            jazz::db::InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap()
        .mergeable_tx_id()
    };
    let edit = block_on(alice.update("notes", row(1), cells("rejected edit"), Default::default()))
        .unwrap();
    connect(&alice, &bob, "alice");
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(first, DurabilityTier::Global),
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
/// Alice's ordinary mergeable insert settles; her refused edit must retain
/// the accepted founder on Alice and on Bob's authority.
#[test]
fn ordinary_mergeable_write_only_retains_accepted_founder_after_refused_edit() {
    ordinary_write_only_case(false);
}
/// Alice's ordinary serializable insert follows the same contract without
/// initialization reservation or sealed publication.
#[test]
fn ordinary_serializable_write_only_retains_accepted_founder_after_refused_edit() {
    ordinary_write_only_case(true);
}

struct TamperOrigins {
    transport: Box<dyn jazz::db::Transport>,
    mode: &'static str,
    hits: std::rc::Rc<std::cell::Cell<usize>>,
}
impl jazz::db::Transport for TamperOrigins {
    fn send(&mut self, mut message: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
        use jazz::db::{Acting, SyncMessage, WireKind};
        if let SyncMessage::Commit {
            kind: WireKind::SerializableOriginsV2 { own, .. },
            acting,
            ..
        } = &mut message
        {
            self.hits.set(self.hits.get() + 1);
            match self.mode {
                "duplicate" => own.push(own[0]),
                // Current positions start at one; zero is the invalid current
                // coordinate, unlike the retired LOCAL_BASE-era literal.
                "invalid-position" => own[0].0 = 0,
                "forged-author" => {
                    *acting = Acting::Attributed {
                        author: author("mallory").canonical().to_owned(),
                    };
                }
                "session-other-author" => {
                    *acting = Acting::Session {
                        author: author("mallory").canonical().to_owned(),
                        claims: BTreeMap::from([("role".into(), Value::String("wrong".into()))]),
                    };
                }
                _ => unreachable!(),
            }
        }
        self.transport.send(message)
    }
    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        self.transport.try_recv()
    }
}

/// Direct structured transport reaches authority shape/principal admission;
/// separate byte fixtures cover canonical decode refusal before admission.
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
        block_on(alice.tx_ref(id).update(
            "notes",
            row(1),
            cells("must reject"),
            Default::default(),
        ))
        .unwrap();
        let receipt = block_on(alice.commit_serializable_handle(id)).unwrap();
        let hits = std::rc::Rc::new(std::cell::Cell::new(0));
        let (up, down) = duplex();
        block_on(alice.connect_upstream(Box::new(TamperOrigins {
            transport: up,
            mode,
            hits: hits.clone(),
        })));
        bob.accept_subscriber(down, author("alice")).unwrap();
        assert!(
            drive(
                &[&alice, &bob],
                alice.wait_for_transaction(receipt.0, DurabilityTier::Global)
            )
            .is_err(),
            "{mode}"
        );
        assert!(
            hits.get() > 0,
            "{mode} never reached the actual origin variant"
        );
        assert_eq!(rows(&bob), vec![(row(1), "founder".into())], "{mode}");
    }
}

/// A non-vouched Session payload cannot replace actual Alice or her real
/// connection claims. This is distinct from forbidden Mallory attribution.
#[test]
fn origin_session_payload_preserves_actual_link_policy_and_provenance() {
    use jazz::tools::{PolicyExpr, TablePolicies};
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(
                                Some(PolicyExpr::True),
                                PolicyExpr::eq_session(
                                    "text",
                                    vec!["claims".into(), "role".into()],
                                ),
                            ),
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
    alice.enable_initialization_journal().unwrap();
    alice.set_identity_claims(
        author("alice"),
        BTreeMap::from([(
            jazz::query::provider_claim_key("role"),
            Value::String("allowed".into()),
        )]),
    );
    let bob = block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([3; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    bob.set_authority();
    let seal = alice
        .seal_initialization_transaction(stage(&alice, 1))
        .unwrap();
    alice.publish_initialization_transaction(seal).unwrap();
    let id = OpenTransactionId::new();
    block_on(alice.begin_serializable(id)).unwrap();
    block_on(
        alice
            .tx_ref(id)
            .update("notes", row(1), cells("allowed"), Default::default()),
    )
    .unwrap();
    let receipt = block_on(alice.commit_serializable_handle(id)).unwrap();
    let hits = std::rc::Rc::new(std::cell::Cell::new(0));
    let (up, down) = duplex();
    block_on(alice.connect_upstream(Box::new(TamperOrigins {
        transport: up,
        mode: "session-other-author",
        hits: hits.clone(),
    })));
    bob.accept_subscriber_with_claims(
        down,
        author("alice"),
        Some(BTreeMap::from([(
            jazz::query::provider_claim_key("role"),
            Value::String("allowed".into()),
        )])),
    )
    .unwrap();
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
    )
    .unwrap();
    assert!(hits.get() > 0);
    assert_eq!(rows(&bob), vec![(row(1), "allowed".into())]);
    let query = bob
        .prepare_query(
            &Query::from("notes")
                .select(["$createdBy.identity.subject", "$updatedBy.identity.subject"]),
        )
        .unwrap();
    let result = block_on(bob.all(&query, ReadOpts::default())).unwrap();
    assert_eq!(result.len(), 1);
    for name in ["$createdBy.identity.subject", "$updatedBy.identity.subject"] {
        let cell = result[0]
            .cells()
            .find(|(field, _)| *field == name)
            .unwrap()
            .1
            .unwrap();
        let text = match cell {
            Value::String(value) => value.as_str(),
            Value::Nullable(Some(value)) => match value.as_ref() {
                Value::String(value) => value.as_str(),
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        };
        assert_eq!(text, "alice");
    }
}

/// A foreign readable original commit is not Mallory's own snapshot prefix.
#[test]
fn origin_foreign_observed_commit_is_not_an_owned_prefix_grant() {
    use jazz::db::{Acting, Outcome, SyncMessage, WireKind};
    use jazz::time::TxTime;
    use jazz::tx::TxId;
    let alice = open(1, "alice", None, false);
    let bob = open(3, "bob", None, true);
    let seal = alice
        .seal_initialization_transaction(stage(&alice, 1))
        .unwrap();
    let original = seal.reservation().transaction_id();
    let receipt = alice.publish_initialization_transaction(seal).unwrap();
    connect(&alice, &bob, "alice");
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
    )
    .unwrap();
    let (mut mallory, down) = duplex();
    bob.accept_subscriber(down, author("mallory")).unwrap();
    let id = TxId::new(TxTime(1), NodeUuid::from_bytes([9; 16]));
    mallory
        .send(SyncMessage::Commit {
            id,
            clock: 11,
            kind: WireKind::SerializableOriginsV2 {
                snapshot: 0,
                reads: vec![],
                own: vec![(17, *original.node.as_bytes(), original.time.0)],
            },
            ops: vec![],
            acting: Acting::Own,
        })
        .unwrap();
    block_on(bob.tick()).unwrap();
    let mut refused = false;
    while let Some(message) = mallory.try_recv() {
        if let SyncMessage::Fate {
            id: got, outcome, ..
        } = message
        {
            if got == id {
                assert!(matches!(outcome, Outcome::Refused { .. }));
                refused = true;
            }
        }
    }
    assert!(refused);
    assert_eq!(rows(&bob), vec![(row(1), "founder".into())]);
}

#[test]
fn accepted_history_uses_reserved_identity_without_exposing_pending_rows() {
    use jazz::query::{col, eq, lit};
    for authority in [false, true] {
        let db = if authority {
            physical_authority(1, "alice", None)
        } else {
            open(1, "alice", None, false)
        };
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

#[test]
fn physical_authority_publication_has_real_global_fate_and_no_replica_role() {
    let db = physical_authority(31, "alice", None);
    let seal = db.seal_initialization_transaction(stage(&db, 1)).unwrap();
    let reserved = seal.reservation();
    assert!(db.is_authority());
    assert_eq!(db.sync_debug(), "not a client");
    assert_eq!(
        db.initialization_transaction_status(&[reserved]).unwrap(),
        [InitializationTransactionStatus::NotObserved]
    );
    assert!(rows(&db).is_empty());
    let receipt = db.publish_initialization_transaction(seal).unwrap();
    assert_ne!(receipt.0, reserved.transaction_id().time.0);
    assert_eq!(
        db.committed_transaction_id(receipt.0).unwrap(),
        reserved.transaction_id()
    );
    let state = db.initialization_transaction_status(&[reserved]).unwrap();
    assert!(
        matches!(&state[0],InitializationTransactionStatus::Complete(state) if state.fate==Fate::Accepted && state.durability==DurabilityTier::Global && state.global_position==Some(receipt.0))
    );
    assert_eq!(db.sync_debug(), "not a client");
    assert_eq!(rows(&db), vec![(row(1), "founder".into())]);
}

#[test]
fn physical_authority_preserves_readset_conflict_and_spent_identity() {
    let db = physical_authority(32, "alice", None);
    let seal = db.seal_initialization_transaction(stage(&db, 1)).unwrap();
    let old = seal.reservation();
    block_on(db.insert(
        "notes",
        cells("competing"),
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
    assert_eq!(rows(&db), vec![(row(1), "competing".into())]);
    assert_eq!(
        db.initialization_transaction_status(&[old]).unwrap(),
        [InitializationTransactionStatus::NotObserved]
    );
}

#[test]
fn physical_authority_rechecks_actual_session_policy_after_seal() {
    use jazz::tools::{PolicyExpr, TablePolicies};
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::eq_session(
                                "text",
                                vec!["claims".into(), "role".into()],
                            )),
                    ),
            )
            .build(),
    );
    let db = block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([33; 16]),
            author: author("alice"),
        },
    )))
    .unwrap();
    db.set_authority();
    db.enable_initialization_journal().unwrap();
    db.set_identity_claims(
        author("alice"),
        BTreeMap::from([(
            jazz::query::provider_claim_key("role"),
            Value::String("founder".into()),
        )]),
    );
    let id = OpenTransactionId::new();
    block_on(db.begin_serializable_for_identity(id, author("alice"))).unwrap();
    db.prepare_initialization_insert(id, "notes", row(1))
        .unwrap();
    block_on(db.tx_ref(id).insert(
        "notes",
        cells("founder"),
        jazz::db::InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let seal = db.seal_initialization_transaction(id).unwrap();
    let reserved = seal.reservation();
    db.set_identity_claims(
        author("alice"),
        BTreeMap::from([(
            jazz::query::provider_claim_key("role"),
            Value::String("revoked".into()),
        )]),
    );
    let error = db.publish_initialization_transaction(seal).unwrap_err();
    // This is an actual policy refusal during the publication's ordinary
    // pre-apply checks, not the narrower apply-time classifier.
    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert!(error.message.contains("INSERT policy denied"), "{error:?}");
    assert!(rows(&db).is_empty());
    assert_eq!(
        db.initialization_transaction_status(&[reserved]).unwrap(),
        [InitializationTransactionStatus::NotObserved]
    );
}

#[test]
fn physical_authority_refuses_system_hybrid_foreign_session_and_closed_owner() {
    let system = open(34, "unused", None, true);
    assert!(system.enable_initialization_journal().is_err());
    assert!(system.is_authority());
    assert_eq!(system.sync_debug(), "not a client");
    let hybrid = physical_authority(35, "alice", None);
    hybrid.become_replica();
    assert!(hybrid.enable_initialization_journal().is_err());
    let db = physical_authority(36, "alice", None);
    let id = OpenTransactionId::new();
    block_on(db.begin_serializable_for_identity(id, author("mallory"))).unwrap();
    assert!(db.seal_initialization_transaction(id).is_err());
    db.abandon_transaction_handle(id).unwrap();
    let seal = db.seal_initialization_transaction(stage(&db, 1)).unwrap();
    block_on(db.close()).unwrap();
    assert!(db.publish_initialization_transaction(seal).is_err());
}

#[test]
fn physical_authority_failed_terminal_storage_rolls_back_rows_and_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = physical_authority(37, "alice", Some(&path));
    let seal = db.seal_initialization_transaction(stage(&db, 1)).unwrap();
    let reserved = seal.reservation();
    let storage = rusqlite::Connection::open(&path).unwrap();
    storage.execute_batch("CREATE TRIGGER fail_authority_fate BEFORE INSERT ON _jazz_transactions BEGIN SELECT RAISE(ABORT,'injected genuine fate failure'); END;").unwrap();
    assert_eq!(
        db.publish_initialization_transaction(seal)
            .unwrap_err()
            .code,
        ErrorCode::Storage
    );
    assert!(rows(&db).is_empty());
    assert_eq!(
        db.initialization_transaction_status(&[reserved]).unwrap(),
        [InitializationTransactionStatus::NotObserved]
    );
    let query = db
        .prepare_query(&Query::from("history.notes").filter(jazz::query::eq(
            jazz::query::col("id"),
            jazz::query::lit(Value::Uuid(row(1).0)),
        )))
        .unwrap();
    assert!(
        block_on(db.all(&query, ReadOpts::default()))
            .unwrap()
            .is_empty()
    );
    storage
        .execute_batch("DROP TRIGGER fail_authority_fate")
        .unwrap();
    let next = db.seal_initialization_transaction(stage(&db, 2)).unwrap();
    assert_ne!(next.reservation(), reserved);
    db.publish_initialization_transaction(next).unwrap();
    assert_eq!(rows(&db), vec![(row(2), "founder".into())]);
}

/// Original-ID aliases must not drop the already-supported rejected-operation
/// payload. This is not retention of a SELECT-denied row in the query cache.
#[test]
fn rejected_initialization_preserves_exact_existing_rejected_operation() {
    use jazz::db::WireWrite;
    use jazz::tools::{PolicyExpr, TablePolicies};
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::False),
                    ),
            )
            .build(),
    );
    let alice = block_on(Db::open(DbConfig::new(
        schema.clone(),
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([41; 16]),
            author: author("alice"),
        },
    )))
    .unwrap();
    alice.enable_initialization_journal().unwrap();
    alice.track_upstream_rejections();
    let bob = block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([42; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    bob.set_authority();
    let seal = alice
        .seal_initialization_transaction(stage(&alice, 1))
        .unwrap();
    let reserved = seal.reservation();
    let receipt = alice.publish_initialization_transaction(seal).unwrap();
    connect(&alice, &bob, "alice");
    assert!(
        drive(
            &[&alice, &bob],
            alice.wait_for_transaction(receipt.0, DurabilityTier::Global)
        )
        .is_err()
    );
    let rejected = alice.rejected_transactions().unwrap();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].seq, receipt.0);
    assert!(rejected[0].serializable);
    assert_eq!(rejected[0].ops.len(), 1);
    assert!(
        matches!(&rejected[0].ops[0].write,WireWrite::Insert {row:written,cells,..} if *written==[1;16] && cells.values().any(|cell|matches!(cell,Value::String(value) if value=="founder")))
    );
    assert!(
        matches!(&alice.initialization_transaction_status(&[reserved]).unwrap()[0],InitializationTransactionStatus::Complete(state) if matches!(state.fate,Fate::Rejected {..}) && state.durability==DurabilityTier::Global)
    );
    assert!(rows(&bob).is_empty());
}

/// A local publication coordinate must not become a second canonical Global
/// identity. This verifies same-node reserved IDs and a different reopened node.
#[test]
fn origin_global_history_records_only_the_actual_original_identity() {
    for reopen in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.sqlite");
        let mut alice = open(1, "alice", Some(&path), false);
        let seal = alice
            .seal_initialization_transaction(stage(&alice, 1))
            .unwrap();
        let original = seal.reservation().transaction_id();
        let receipt = alice.publish_initialization_transaction(seal).unwrap();
        assert_ne!(receipt.0, original.time.0);
        if reopen {
            drop(alice);
            alice = open(2, "alice", Some(&path), false);
        }
        let bob = open(3, "bob", None, true);
        connect(&alice, &bob, "alice");
        drive(
            &[&alice, &bob],
            alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap();
        let query = alice
            .prepare_query(&Query::from("history.notes").filter(jazz::query::eq(
                jazz::query::col("id"),
                jazz::query::lit(Value::Uuid(row(1).0)),
            )))
            .unwrap();
        let history = block_on(alice.all(&query, ReadOpts::default())).unwrap();
        assert_eq!(history.len(), 1);
        let identity = history[0]
            .cells()
            .find(|(name, _)| *name == "$transactionId")
            .unwrap()
            .1
            .cloned()
            .unwrap();
        assert_eq!(
            identity,
            Value::Tuple(vec![
                Value::U64(original.time.0),
                Value::Uuid(original.node.0)
            ])
        );
        let storage = rusqlite::Connection::open(&path).unwrap();
        let mut statement=storage.prepare("SELECT hex(n.node),t.tx FROM _jazz_transactions AS t JOIN _jazz_nodes AS n ON n.n=t.n WHERE t.t IS NOT NULL").unwrap();
        let recorded = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            recorded,
            vec![(
                hex::encode_upper(original.node.as_bytes()),
                original.time.0 as i64
            )]
        );
        let pending: i64 = storage
            .query_row(
                "SELECT COUNT(*) FROM _jazz_initialization_pending_v1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pending, 0);
    }
}
