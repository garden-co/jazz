//! Native WAL reader openings preserve the ordinary owner and permission path.
#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

use jazz::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, FileOptions, InsertOptions, ReadOpts,
    SubscriptionEvent, SubscriptionStream, UpdateOptions, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, contains, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

use crate::common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

fn schema(policy: PolicyExpr) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .policies(read_and_allow_all_writes(policy)),
            )
            .table(
                TableSchemaBuilder::new("probes")
                    .column("n", ColumnType::BigInt)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("note", "notes")
                    .column("member", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(schema: JazzSchema) -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let db = block_on(Db::open_at(
        DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x4d; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        &dir.path().join("jazz.sqlite"),
        FileOptions::default(),
    ))
    .unwrap();
    db.set_authority();
    db.enable_concurrent_reads(2, Arc::new(|| {})).unwrap();
    (dir, db)
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x6a; 16])
}

fn note(db: &Db, body: &str, owner: &str) -> RowUuid {
    block_on(db.insert(
        "notes",
        BTreeMap::from([
            ("body".to_owned(), Value::String(body.to_owned())),
            ("owner".to_owned(), Value::String(owner.to_owned())),
        ]),
        InsertOptions::default(),
    ))
    .unwrap()
    .row_uuid()
}

fn subscribe(db: &Db, query: Query) -> SubscriptionStream {
    let prepared = db.prepare_query(&query).unwrap();
    block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), alice())).unwrap()
}

fn next(db: &Db, stream: &mut SubscriptionStream) -> SubscriptionEvent {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        block_on(db.tick()).unwrap();
        if let Some(event) = stream.try_next_event() {
            return event;
        }
        assert!(Instant::now() < deadline, "reader opening did not complete");
        std::thread::sleep(Duration::from_millis(1));
    }
}

struct Paused {
    entered: mpsc::Receiver<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl Drop for Paused {
    fn drop(&mut self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }
}

fn pause_first(db: &Db) -> Paused {
    let first = AtomicBool::new(true);
    let (entered, receiver) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let worker_release = release.clone();
    db.set_read_worker_hook_for_test(Arc::new(move || {
        if !first.swap(false, Ordering::AcqRel) {
            return;
        }
        entered.send(()).unwrap();
        let (lock, changed) = &*worker_release;
        let (released, _) = changed
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |released| {
                !*released
            })
            .unwrap();
        assert!(*released, "test did not release its paused reader");
    }));
    Paused {
        entered: receiver,
        release,
    }
}

/// Alice's paused opening holds a real SQLite snapshot; Bob can commit an
/// unrelated write and Carol can read it before Alice is released.
/// alice: pin -> pause ---------------------> result
/// bob:               insert -> carol: read
/// The test hook only makes that interleaving deterministic.
#[test]
fn a_paused_opening_does_not_block_an_unrelated_write_or_reader() {
    let (_dir, db) = open(schema(PolicyExpr::True));
    note(&db, "alice's note", "alice");
    let paused = pause_first(&db);
    let mut slow = subscribe(&db, Query::from("notes"));
    block_on(db.tick()).unwrap();
    paused.entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let written = block_on(db.insert(
        "probes",
        BTreeMap::from([("n".to_owned(), Value::I64(42))]),
        InsertOptions::default(),
    ))
    .unwrap()
    .row_uuid();
    let mut quick = subscribe(&db, Query::from("probes"));
    let SubscriptionEvent::Delta { added, .. } = next(&db, &mut quick) else {
        panic!("opening delta")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row.row_uuid(), written);
    assert_eq!(added[0].row.cell("n"), Some(Value::I64(42)));
    assert!(slow.try_next_event().is_none());
    drop(paused);
    let SubscriptionEvent::Delta { added, .. } = next(&db, &mut slow) else {
        panic!("opening delta")
    };
    assert_eq!(added.len(), 1);
}

/// Bob revokes Alice's row access after her snapshot is pinned. Alice's
/// earlier worker result must never publish; her opening is evaluated again.
#[test]
fn a_permission_change_discards_an_in_flight_opening() {
    let (_dir, db) = open(schema(session_eq(
        "owner",
        &["user", "identity", "subject"],
    )));
    let row = note(&db, "private", &alice().principal_parts().1);
    let paused = pause_first(&db);
    let mut stream = subscribe(&db, Query::from("notes"));
    block_on(db.tick()).unwrap();
    paused.entered.recv_timeout(Duration::from_secs(5)).unwrap();
    block_on(db.update(
        "notes",
        row,
        BTreeMap::from([("owner".to_owned(), Value::String("mallory".to_owned()))]),
        UpdateOptions::default(),
    ))
    .unwrap();
    drop(paused);
    let SubscriptionEvent::Delta { added, .. } = next(&db, &mut stream) else {
        panic!("opening delta")
    };
    assert!(
        added.is_empty(),
        "a revoked private row must never be published"
    );
}

/// Alice's claim changes while a read is pinned. Her old claim's result
/// is discarded before publication, even though the stored rows did not change.
#[test]
fn a_claim_change_discards_an_in_flight_opening() {
    let (_dir, db) = open(schema(session_eq("owner", &["claims", "region"])));
    note(&db, "private", "west");
    let claims = |region: &str| {
        BTreeMap::from([(
            "\0claims:region".to_owned(),
            Value::String(region.to_owned()),
        )])
    };
    db.set_identity_claims(alice(), claims("west"));
    let paused = pause_first(&db);
    let mut stream = subscribe(&db, Query::from("notes"));
    block_on(db.tick()).unwrap();
    paused.entered.recv_timeout(Duration::from_secs(5)).unwrap();
    db.set_identity_claims(alice(), claims("east"));
    drop(paused);
    let SubscriptionEvent::Delta { added, .. } = next(&db, &mut stream) else {
        panic!("opening delta")
    };
    assert!(
        added.is_empty(),
        "the old claim must not authorize publication"
    );
}

/// Bob revokes Alice's membership while her note snapshot is pinned.
/// The unchanged root row must not escape a changed permission dependency.
#[test]
fn revoking_a_policy_dependency_discards_an_in_flight_opening() {
    let (_dir, db) = open(schema(exists(
        "memberships",
        vec![
            outer_eq("note", "id"),
            session_eq("member", &["user", "identity", "subject"]),
        ],
    )));
    let row = note(&db, "private", "alice");
    let membership = block_on(db.insert(
        "memberships",
        BTreeMap::from([
            ("note".to_owned(), Value::Uuid(row.0)),
            (
                "member".to_owned(),
                Value::String(alice().principal_parts().1),
            ),
        ]),
        InsertOptions::default(),
    ))
    .unwrap()
    .row_uuid();
    let paused = pause_first(&db);
    let mut stream = subscribe(&db, Query::from("notes"));
    block_on(db.tick()).unwrap();
    paused.entered.recv_timeout(Duration::from_secs(5)).unwrap();
    block_on(db.delete("memberships", membership, DeleteOptions::default())).unwrap();
    drop(paused);
    let SubscriptionEvent::Delta { added, .. } = next(&db, &mut stream) else {
        panic!("opening delta")
    };
    assert!(
        added.is_empty(),
        "a revoked membership must not authorize publication"
    );
}

/// Bob activates a denying schema while Alice's opening is pinned. The
/// worker's older policy must not survive the schema activation.
#[test]
fn a_schema_policy_change_discards_an_in_flight_opening() {
    let (_dir, db) = open(schema(PolicyExpr::True));
    note(&db, "private", "alice");
    let paused = pause_first(&db);
    let mut stream = subscribe(&db, Query::from("notes"));
    block_on(db.tick()).unwrap();
    paused.entered.recv_timeout(Duration::from_secs(5)).unwrap();
    block_on(db.activate_schema(schema(PolicyExpr::False))).unwrap();
    drop(paused);
    let SubscriptionEvent::Delta { added, .. } = next(&db, &mut stream) else {
        panic!("opening delta")
    };
    assert!(
        added.is_empty(),
        "an older schema must not authorize publication"
    );
}

/// Alice and Carol use scratch predicates over chunked text on separate
/// read connections while Bob commits to the main database. Neither scratch
/// table may acquire the main database's writer lock or leak answers.
#[test]
fn chunked_predicates_use_connection_local_scratch_and_consistent_chunks() {
    let (_dir, db) = open(schema(PolicyExpr::True));
    let body = "alpha beta ".repeat(9_000);
    note(&db, &body, "alice");
    let paused = pause_first(&db);
    let mut alice_read = subscribe(
        &db,
        Query::from("notes").filter(contains(col("body"), lit("alpha"))),
    );
    block_on(db.tick()).unwrap();
    paused.entered.recv_timeout(Duration::from_secs(5)).unwrap();
    block_on(db.insert(
        "probes",
        BTreeMap::from([("n".to_owned(), Value::I64(1))]),
        InsertOptions::default(),
    ))
    .unwrap();
    let mut carol_read = subscribe(
        &db,
        Query::from("notes").filter(contains(col("body"), lit("beta"))),
    );
    let SubscriptionEvent::Delta { added, .. } = next(&db, &mut carol_read) else {
        panic!("opening delta")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row.cell("body"), Some(Value::String(body.clone())));
    drop(paused);
    let SubscriptionEvent::Delta { added, .. } = next(&db, &mut alice_read) else {
        panic!("opening delta")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row.cell("body"), Some(Value::String(body)));
}

/// Alice abandons a pinned read. Bob's later read still completes, and
/// closing their store releases all worker connections so its WAL is removed.
#[test]
fn canceled_openings_do_not_keep_the_store_or_wal_open() {
    let (dir, db) = open(schema(PolicyExpr::True));
    note(&db, "hello", "alice");
    let paused = pause_first(&db);
    let stream = subscribe(&db, Query::from("notes"));
    block_on(db.tick()).unwrap();
    paused.entered.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(stream);
    block_on(db.tick()).unwrap();
    drop(paused);
    let mut bob = subscribe(&db, Query::from("notes"));
    let SubscriptionEvent::Delta { added, .. } = next(&db, &mut bob) else {
        panic!("opening delta")
    };
    assert_eq!(added.len(), 1);
    drop(bob);
    block_on(db.close()).unwrap();
    drop(db);
    assert!(!dir.path().join("jazz.sqlite-wal").exists());
}

/// A panicking worker rejects Alice's opening without taking Bob's writer
/// or later reader down. The hook injects a failure at the worker boundary.
#[test]
fn a_worker_panic_rejects_its_read_and_later_work_still_runs() {
    let (_dir, db) = open(schema(PolicyExpr::True));
    note(&db, "hello", "alice");
    let first = AtomicBool::new(true);
    db.set_read_worker_hook_for_test(Arc::new(move || {
        assert!(
            !first.swap(false, Ordering::AcqRel),
            "injected worker failure"
        );
    }));
    let mut alice_read = subscribe(&db, Query::from("notes"));
    let SubscriptionEvent::Rejected { reason } = next(&db, &mut alice_read) else {
        panic!("worker failure must reject the read")
    };
    assert!(reason.contains("read worker panicked"));
    block_on(db.insert(
        "probes",
        BTreeMap::from([("n".to_owned(), Value::I64(42))]),
        InsertOptions::default(),
    ))
    .unwrap();
    let mut bob_read = subscribe(&db, Query::from("probes"));
    let SubscriptionEvent::Delta { added, .. } = next(&db, &mut bob_read) else {
        panic!("opening delta")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row.cell("n"), Some(Value::I64(42)));
}
