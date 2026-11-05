//! Paired E2EE reads qualify stored content with authority evidence at one cut.
mod common;
use jazz::db::{
    BindingSettledRead, Db, DbConfig, DbIdentity, Error, ErrorCode, ReadOpts, ReadTier,
    RemoteLinkHint, SerializedReadResult, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

fn open(node: u8, authority: bool) -> Db {
    open_at(node, authority, None)
}
fn open_at(node: u8, authority: bool, path: Option<&std::path::Path>) -> Db {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("refs")
                    .column("text", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    let db = match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap();
    if authority {
        db.set_authority();
    }
    db
}
fn connect(client: &Db, server: &Db) {
    let (a, b) = duplex();
    block_on(client.connect_upstream(a));
    server.accept_subscriber(b, AuthorSubject::SYSTEM);
}
fn turn(dbs: &[&Db]) {
    for db in dbs {
        block_on(db.tick()).unwrap();
    }
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
        turn(dbs);
    }
    panic!("operation did not finish")
}
fn cells(s: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("text".into(), Value::String(s.into()))])
}
fn insert(db: &Db, table: &str, s: &str) -> jazz::db::WriteHandle {
    block_on(db.insert(table, cells(s), Default::default())).unwrap()
}
fn begin(db: &Db) -> OpenTransactionId {
    let id = OpenTransactionId::new();
    block_on(db.begin_serializable(id)).unwrap();
    id
}
fn options(local: bool, deleted: bool) -> ReadOpts {
    let mut opts = ReadOpts::for_read_tier(if local {
        ReadTier::LocalOnly
    } else {
        ReadTier::Remote
    });
    opts.read_view.include_deleted = deleted;
    opts
}
fn paired(
    db: &Db,
    id: OpenTransactionId,
    table: &str,
    local: bool,
    at_begin: bool,
    deleted: bool,
) -> impl Future<Output = Result<BindingSettledRead, Error>> + 'static {
    db.all_settled(
        db.prepare_query(&Query::from(table)).unwrap(),
        options(local, deleted),
        id,
        at_begin,
        None,
    )
    .unwrap()
}
fn texts(read: &BindingSettledRead) -> Vec<String> {
    read.rows
        .iter()
        .map(|r| {
            let Value::String(text) = r.cells().find(|(n, _)| *n == "text").unwrap().1.unwrap()
            else {
                panic!("text")
            };
            text.clone()
        })
        .collect()
}
fn fetch(db: &Db, dbs: &[&Db], query: Query) {
    let bytes = postcard::to_allocvec(&query).unwrap();
    let read = db.all_serialized_query(
        &bytes,
        options(false, true),
        None,
        None,
        None,
        false,
        || false,
        |_| {},
    );
    assert!(matches!(
        drive(dbs, read).unwrap(),
        SerializedReadResult::Rows(_)
    ));
}

/// Alice cold-reads Bob's authority state and commits no writes. The paired
/// read's actual read set must reach the authority and reject a later change.
#[test]
fn cold_online_pair_and_read_only_validation() {
    for change in [false, true] {
        let server = open(3, true);
        let row = insert(&server, "notes", "accepted");
        let alice = open(1, false);
        connect(&alice, &server);
        let tx = begin(&alice);
        let read = drive(
            &[&alice, &server],
            paired(&alice, tx, "notes", false, false, false),
        )
        .unwrap();
        assert_eq!(texts(&read), ["accepted"]);
        assert_eq!(read.settlements[0].0.time.0, row.mergeable_tx_id());
        assert_eq!(read.settlements[0].0.node, NodeUuid::from_bytes([3; 16]));
        assert_eq!(read.settlements[0].1.0, row.mergeable_tx_id());
        if change {
            block_on(server.update(
                "notes",
                row.row_uuid(),
                cells("changed"),
                Default::default(),
            ))
            .unwrap();
        }
        let receipt = block_on(alice.commit_serializable_handle(tx)).unwrap();
        let accepted = drive(
            &[&alice, &server],
            alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
        );
        if change {
            assert_eq!(accepted.unwrap_err().code, ErrorCode::TransactionConflict);
        } else {
            accepted.unwrap();
        }
    }
}

/// A deletion changes row state, but Alice still gets the original content
/// transaction and position. Its paired read retains ordinary tombstone access.
#[test]
fn deletion_keeps_the_content_transaction() {
    let server = open(3, true);
    let first = insert(&server, "notes", "original");
    block_on(server.delete("notes", first.row_uuid(), Default::default())).unwrap();
    let alice = open(1, false);
    connect(&alice, &server);
    let tx = begin(&alice);
    let read = drive(
        &[&alice, &server],
        paired(&alice, tx, "notes", false, false, true),
    )
    .unwrap();
    assert_eq!(texts(&read), ["original"]);
    assert_eq!(read.settlements[0].0.time.0, first.mergeable_tx_id());
    assert_eq!(read.settlements[0].1.0, first.mergeable_tx_id());
}

/// Alice's offline observation ignores her pending insert/update/delete, and
/// shares the accepted-at-begin snapshot across both queried tables.
#[test]
fn local_observation_excludes_pending_content_and_deletion() {
    let server = open(3, true);
    let note = insert(&server, "notes", "accepted");
    let reference = insert(&server, "refs", "reference");
    let alice = open(1, false);
    connect(&alice, &server);
    fetch(&alice, &[&alice, &server], Query::from("notes"));
    fetch(&alice, &[&alice, &server], Query::from("refs"));
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    block_on(alice.update(
        "notes",
        note.row_uuid(),
        cells("pending"),
        Default::default(),
    ))
    .unwrap();
    insert(&alice, "notes", "pending insert");
    block_on(alice.delete("refs", reference.row_uuid(), Default::default())).unwrap();
    let tx = begin(&alice);
    let notes = block_on(paired(&alice, tx, "notes", true, true, false)).unwrap();
    let refs = block_on(paired(&alice, tx, "refs", true, true, false)).unwrap();
    assert_eq!(texts(&notes), ["accepted"]);
    assert_eq!(texts(&refs), ["reference"]);
    alice.abandon_transaction_handle(tx).unwrap();
}

/// Bob's rows were already accepted upstream but unknown when Alice began.
/// Receiving evidence afterwards must not backfill her local begin snapshot.
#[test]
fn begin_snapshot_excludes_late_arriving_old_evidence() {
    let server = open(3, true);
    insert(&server, "notes", "old upstream content");
    let alice = open(1, false);
    connect(&alice, &server);
    fetch(&alice, &[&alice, &server], Query::from("notes").limit(0));
    let tx = begin(&alice);
    fetch(&alice, &[&alice, &server], Query::from("notes"));
    assert!(
        block_on(paired(&alice, tx, "notes", true, true, false))
            .unwrap()
            .rows
            .is_empty()
    );
    let next = begin(&alice);
    assert_eq!(
        texts(&block_on(paired(&alice, next, "notes", true, true, false)).unwrap()),
        ["old upstream content"]
    );
}

/// A live authority read may hydrate new evidence; opting into accepted-at-begin
/// must refuse that evidence if Alice did not have it when she opened the batch.
#[test]
fn online_begin_requirement_refuses_late_evidence() {
    let server = open(3, true);
    insert(&server, "notes", "accepted");
    let alice = open(1, false);
    connect(&alice, &server);
    let tx = begin(&alice);
    let error = drive(
        &[&alice, &server],
        paired(&alice, tx, "notes", false, true, false),
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert!(block_on(alice.commit_serializable_handle(tx)).is_err());
}

/// Alice starts both reads together. The second waits for the first to pin a
/// cut, then reads Bob's older reference even after he updates it meanwhile.
#[test]
fn concurrent_queries_share_one_authority_snapshot() {
    let server = open(3, true);
    insert(&server, "notes", "first");
    let reference = insert(&server, "refs", "before");
    let alice = open(1, false);
    connect(&alice, &server);
    let tx = begin(&alice);
    let first = paired(&alice, tx, "notes", false, false, false);
    let second = paired(&alice, tx, "refs", false, false, false);
    let mut second = pin!(second);
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(texts(&drive(&[&alice, &server], first).unwrap()), ["first"]);
    block_on(server.update(
        "refs",
        reference.row_uuid(),
        cells("after"),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(
        texts(&drive(&[&alice, &server], second).unwrap()),
        ["before"]
    );
}

/// A local server's writer origins are not acceptance. Alice's offline
/// observation omits them and her online request cannot promote local coverage.
#[test]
fn nonauthority_origins_never_become_accepted_evidence() {
    let owner = open(2, false);
    insert(&owner, "notes", "only local");
    let alice = open(1, false);
    connect(&alice, &owner);
    fetch(&alice, &[&alice, &owner], Query::from("notes"));
    let local = begin(&alice);
    assert!(
        block_on(paired(&alice, local, "notes", true, true, false))
            .unwrap()
            .rows
            .is_empty()
    );
    let remote = begin(&alice);
    assert_eq!(
        drive(
            &[&alice, &owner],
            paired(&alice, remote, "notes", false, false, false)
        )
        .unwrap_err()
        .code,
        ErrorCode::NotObserved
    );
}

/// Alice's paired read traverses a trusted storage relay, which must obtain
/// authority coverage instead of answering from its initially empty cache.
#[test]
fn paired_read_through_relay_waits_for_authority() {
    let server = open(3, true);
    insert(&server, "notes", "remote");
    let owner = open(2, false);
    connect(&owner, &server);
    let alice = open(1, false);
    connect(&alice, &owner);
    let tx = begin(&alice);
    let read = drive(
        &[&alice, &owner, &server],
        paired(&alice, tx, "notes", false, false, false),
    )
    .unwrap();
    assert_eq!(texts(&read), ["remote"]);
    assert_eq!(read.settlements[0].0.node, NodeUuid::from_bytes([3; 16]));
}

/// Reads register before polling. Alice cannot ignore, cancel or race one and
/// then commit a transaction whose read set silently excludes that decision.
#[test]
fn unfinished_cancelled_and_failed_reads_block_commit() {
    for action in ["unfinished", "cancelled", "provisional"] {
        let db = open(3, true);
        let row = insert(&db, "notes", "accepted");
        let tx = begin(&db);
        if action == "provisional" {
            block_on(db.serializable_tx_ref(tx).update(
                "notes",
                row.row_uuid(),
                cells("staged"),
                Default::default(),
            ))
            .unwrap();
        }
        let read = paired(&db, tx, "notes", false, false, false);
        match action {
            "unfinished" => {
                assert!(block_on(db.commit_serializable_handle(tx)).is_err());
                assert!(block_on(read).is_err());
            }
            "cancelled" => {
                drop(read);
                assert!(block_on(db.commit_serializable_handle(tx)).is_err());
            }
            _ => {
                assert_eq!(block_on(read).unwrap_err().code, ErrorCode::NotObserved);
                assert!(block_on(db.commit_serializable_handle(tx)).is_err());
            }
        }
    }
}

/// An empty accepted result is a read decision too: Bob inserting a phantom
/// before Alice's read-only commit must cause an authority conflict.
#[test]
fn empty_pair_records_the_phantom_read_set() {
    let server = open(3, true);
    let alice = open(1, false);
    connect(&alice, &server);
    let tx = begin(&alice);
    assert!(
        drive(
            &[&alice, &server],
            paired(&alice, tx, "notes", false, false, false)
        )
        .unwrap()
        .rows
        .is_empty()
    );
    insert(&server, "notes", "phantom");
    let receipt = block_on(alice.commit_serializable_handle(tx)).unwrap();
    assert_eq!(
        drive(
            &[&alice, &server],
            alice.wait_for_transaction(receipt.0, DurabilityTier::Global)
        )
        .unwrap_err()
        .code,
        ErrorCode::TransactionConflict
    );
}

/// Alice's cancelled or rolled-back first read releases Bob's queued waiter;
/// reopening the same opaque id cannot attach either old read to the new batch.
#[test]
fn cancelled_and_closed_queues_do_not_contaminate_reused_ids() {
    for rollback in [false, true] {
        let db = open(3, true);
        insert(&db, "notes", "accepted");
        let tx = begin(&db);
        let first = paired(&db, tx, "notes", false, false, false);
        let second = paired(&db, tx, "notes", false, false, false);
        if rollback {
            db.abandon_transaction_handle(tx).unwrap();
            block_on(db.begin_serializable(tx)).unwrap();
        }
        drop(first);
        assert!(block_on(second).is_err());
        if rollback {
            assert_eq!(
                texts(&block_on(paired(&db, tx, "notes", false, false, false)).unwrap()),
                ["accepted"]
            );
            block_on(db.commit_serializable_handle(tx)).unwrap();
        } else {
            assert!(block_on(db.commit_serializable_handle(tx)).is_err());
        }
    }
}

/// The authority's selected large content must hydrate through normal chunk
/// transport, both directly to Alice and through Bob's initially cold relay.
#[test]
fn paired_large_content_hydrates_directly_and_through_a_relay() {
    for relay in [false, true] {
        let server = open(3, true);
        let large: String = (0..300_000)
            .map(|n| (b'a' + (n * 17 % 26) as u8) as char)
            .collect();
        let write = insert(&server, "notes", &large);
        let owner = open(2, false);
        connect(&owner, &server);
        let alice = open(1, false);
        connect(&alice, if relay { &owner } else { &server });
        let tx = begin(&alice);
        let read = drive(
            &[&alice, &owner, &server],
            paired(&alice, tx, "notes", false, false, false),
        )
        .unwrap();
        assert_eq!(texts(&read), [large]);
        assert_eq!(read.settlements[0].0.time.0, write.mergeable_tx_id());
    }
}

/// Alice restarts offline and observes the exact accepted content transaction.
/// A later identical payload from Bob must not replace her begin-time witness.
#[test]
fn accepted_evidence_survives_restart_and_keeps_the_begin_witness() {
    let server = open(3, true);
    let first = insert(&server, "notes", "same");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    {
        let alice = open_at(1, false, Some(&path));
        connect(&alice, &server);
        fetch(&alice, &[&alice, &server], Query::from("notes"));
    }
    let alice = open_at(1, false, Some(&path));
    let tx = begin(&alice);
    assert_eq!(
        block_on(paired(&alice, tx, "notes", true, true, false))
            .unwrap()
            .settlements[0]
            .0
            .time
            .0,
        first.mergeable_tx_id()
    );
    let second =
        block_on(server.update("notes", first.row_uuid(), cells("same"), Default::default()))
            .unwrap();
    connect(&alice, &server);
    fetch(&alice, &[&alice, &server], Query::from("notes"));
    assert_eq!(
        block_on(paired(&alice, tx, "notes", true, true, false))
            .unwrap()
            .settlements[0]
            .0
            .time
            .0,
        first.mergeable_tx_id()
    );
    let next = begin(&alice);
    let warm = drive(
        &[&alice, &server],
        paired(&alice, next, "notes", false, true, false),
    )
    .unwrap();
    assert_eq!(warm.settlements[0].0.time.0, second.mergeable_tx_id());
}

/// Accepted evidence is durable byte-level state, so SQL fault injection is
/// needed to prove Alice refuses malformed/trailing values rather than guessing.
#[test]
fn corrupt_accepted_evidence_fails_closed_after_restart() {
    let mut trailing = vec![1];
    trailing.extend([3; 16]);
    trailing.extend([1, 0]);
    for bytes in [vec![0xff], vec![1, 0], vec![0; 18], trailing] {
        let server = open(3, true);
        insert(&server, "notes", "accepted");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.sqlite");
        {
            let alice = open_at(1, false, Some(&path));
            connect(&alice, &server);
            fetch(&alice, &[&alice, &server], Query::from("notes"));
        }
        let sql = rusqlite::Connection::open(&path).unwrap();
        sql.execute("UPDATE _jazz_accepted_rows_v1 SET content=?1", [bytes])
            .unwrap();
        drop(sql);
        let alice = open_at(1, false, Some(&path));
        let tx = begin(&alice);
        assert_eq!(
            block_on(paired(&alice, tx, "notes", true, true, false))
                .unwrap_err()
                .code,
            ErrorCode::Storage
        );
        assert!(block_on(alice.commit_serializable_handle(tx)).is_err());
    }
}

fn user(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}
fn claims(value: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        jazz::query::provider_claim_key("access"),
        Value::String(value.into()),
    )])
}
fn scoped_open(node: u8, authority: bool) -> Db {
    let policy =
        jazz::tools::PolicyExpr::eq_session("text", vec!["claims".into(), "access".into()]);
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(common::read_and_allow_all_writes(policy)),
            )
            .build(),
    );
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    if authority {
        db.set_authority();
    }
    db
}
fn scoped_pair(
    db: &Db,
    tx: OpenTransactionId,
    value: &str,
) -> Result<impl Future<Output = Result<BindingSettledRead, Error>> + 'static, Error> {
    db.all_settled(
        db.prepare_query(&Query::from("notes"))
            .unwrap()
            .with_identity_claims(user("alice"), claims(value)),
        options(false, false),
        tx,
        false,
        Some(user("alice")),
    )
}

/// Alice's authenticated scope works over her ordinary connection. Neither
/// changed claims nor Mallory's connection may impersonate Alice's scope.
#[test]
fn accepted_requests_enforce_the_authenticated_connection_scope() {
    for (connection, request, allowed) in [
        ("alice", "alice", true),
        ("alice", "bob", false),
        ("mallory", "alice", false),
    ] {
        let server = scoped_open(3, true);
        insert(&server, "notes", "alice");
        insert(&server, "notes", "bob");
        let alice = scoped_open(1, false);
        let (a, b) = duplex();
        block_on(alice.connect_upstream(a));
        server.accept_subscriber_with_claims(b, user(connection), Some(claims("alice")));
        let tx = OpenTransactionId::new();
        block_on(alice.begin_serializable_for_identity(tx, user("alice"))).unwrap();
        let result = drive(
            &[&alice, &server],
            scoped_pair(&alice, tx, request).unwrap(),
        );
        if allowed {
            assert_eq!(texts(&result.unwrap()), ["alice"]);
            let receipt = block_on(alice.commit_serializable_handle(tx)).unwrap();
            drive(
                &[&alice, &server],
                alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
            )
            .unwrap();
        } else {
            assert_eq!(result.unwrap_err().code, ErrorCode::WriteRejected);
            assert!(block_on(alice.commit_serializable_handle(tx)).is_err());
        }
    }
}

/// A trusted backend may delegate Alice's claims. Ambient changes after her
/// read cannot change her commit; mixing explicit scopes poisons the batch.
#[test]
fn trusted_delegation_freezes_claims_for_reads_and_commit() {
    let server = scoped_open(3, true);
    insert(&server, "notes", "alice");
    insert(&server, "notes", "bob");
    let backend = scoped_open(1, false);
    connect(&backend, &server);
    let tx = OpenTransactionId::new();
    block_on(backend.begin_serializable_for_identity(tx, user("alice"))).unwrap();
    assert_eq!(
        texts(
            &drive(
                &[&backend, &server],
                scoped_pair(&backend, tx, "alice").unwrap()
            )
            .unwrap()
        ),
        ["alice"]
    );
    backend.set_identity_claims(user("alice"), claims("bob"));
    let ordinary = backend.prepare_query(&Query::from("notes")).unwrap();
    let rows = drive(
        &[&backend, &server],
        backend.serializable_tx_ref(tx).all_prepared(&ordinary),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    let receipt = block_on(backend.commit_serializable_handle(tx)).unwrap();
    drive(
        &[&backend, &server],
        backend.wait_for_transaction(receipt.0, DurabilityTier::Global),
    )
    .unwrap();
    let mixed = OpenTransactionId::new();
    block_on(backend.begin_serializable_for_identity(mixed, user("alice"))).unwrap();
    let first = scoped_pair(&backend, mixed, "alice").unwrap();
    assert_eq!(
        scoped_pair(&backend, mixed, "bob").err().unwrap().code,
        ErrorCode::WriteRejected
    );
    assert!(drive(&[&backend, &server], first).is_err());
    assert!(block_on(backend.commit_serializable_handle(mixed)).is_err());
}

/// Alice cold-reads an ordered window through Bob's relay. The prefix needed
/// for offset evaluation is hydrated, while her read set retains the window.
#[test]
fn cold_ordered_windows_preserve_authority_order_and_read_sets() {
    for relay in [false, true] {
        let server = open(3, true);
        for s in ["a", "b", "c", "d"] {
            insert(&server, "notes", s);
        }
        let owner = open(2, false);
        connect(&owner, &server);
        let alice = open(1, false);
        connect(&alice, if relay { &owner } else { &server });
        let tx = begin(&alice);
        let query = Query::from("notes")
            .order_by("text", jazz::query::OrderDirection::Asc)
            .offset(1)
            .limit(2);
        let read = alice
            .all_settled(
                alice.prepare_query(&query).unwrap(),
                options(false, false),
                tx,
                false,
                None,
            )
            .unwrap();
        assert_eq!(
            texts(&drive(&[&alice, &owner, &server], read).unwrap()),
            ["b", "c"]
        );
        if !relay {
            let receipt = block_on(alice.commit_serializable_handle(tx)).unwrap();
            drive(
                &[&alice, &server],
                alice.wait_for_transaction(receipt.0, DurabilityTier::Global),
            )
            .unwrap();
        }
    }
}

/// Alice has Bob's accepted note but only a pending grant. Local observation
/// must apply its accepted cut to the policy dependency as well as the note.
#[test]
fn local_observation_cannot_use_pending_policy_evidence() {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(common::read_and_allow_all_writes(common::exists(
                        "grants",
                        vec![common::outer_eq("note", "id")],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("grants")
                    .fk_column("note", "notes")
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    let open = |node| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap()
    };
    let server = open(3);
    server.set_authority();
    let note = insert(&server, "notes", "accepted");
    let alice = open(1);
    connect(&alice, &server);
    fetch(&alice, &[&alice, &server], Query::from("notes"));
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    block_on(alice.insert(
        "grants",
        BTreeMap::from([("note".into(), Value::Uuid(note.row_uuid().0))]),
        Default::default(),
    ))
    .unwrap();
    let tx = OpenTransactionId::new();
    block_on(alice.begin_serializable_for_identity(tx, user("alice"))).unwrap();
    let read = alice
        .all_settled(
            alice.prepare_query(&Query::from("notes")).unwrap(),
            options(true, false),
            tx,
            true,
            Some(user("alice")),
        )
        .unwrap();
    assert!(block_on(read).unwrap().rows.is_empty());
}

/// Mallory cannot upload authority evidence. Invalid and contradictory proofs
/// from an upstream fail explicitly; transport injection tests this boundary.
#[test]
fn accepted_evidence_rejects_invalid_conflicting_and_wrong_direction_messages() {
    use jazz::db::{AcceptedContent, AcceptedRow, SyncMessage};
    let message = |state, content| SyncMessage::AcceptedRowsV1 {
        rows: vec![AcceptedRow {
            table: "notes".into(),
            id: [0x22; 16],
            key: vec![],
            row_seq: state,
            content: AcceptedContent {
                seq: content,
                node: [3; 16],
                tx: 1,
            },
        }],
    };
    for (state, content) in [(3, 0), (3, 4), (1 << 52, 1)] {
        let alice = open(1, false);
        let (a, mut b) = duplex();
        block_on(alice.connect_upstream(a));
        b.send(message(state, content)).unwrap();
        assert_eq!(
            block_on(alice.tick()).unwrap_err().code,
            ErrorCode::Protocol
        );
    }
    let alice = open(1, false);
    let (a, mut b) = duplex();
    block_on(alice.connect_upstream(a));
    b.send(message(3, 1)).unwrap();
    block_on(alice.tick()).unwrap();
    b.send(message(3, 2)).unwrap();
    assert!(
        block_on(alice.tick())
            .unwrap_err()
            .message
            .contains("conflicting accepted-row-v1")
    );
    let server = open(3, true);
    let (mut a, b) = duplex();
    server.accept_subscriber(b, user("mallory"));
    a.send(message(3, 1)).unwrap();
    assert!(
        block_on(server.tick())
            .unwrap_err()
            .message
            .contains("server received AcceptedRowsV1")
    );
}

/// Bob retransmitting proof cannot move Alice's begin frontier. SQL inspection
/// is necessary to pin the stored bytes and distinguish duplicate admission.
#[test]
fn duplicate_evidence_preserves_the_admission_ordinal_and_storage_bytes() {
    let server = open(3, true);
    server.seed_seq_floor(126);
    insert(&server, "notes", "accepted");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let alice = open_at(1, false, Some(&path));
    connect(&alice, &server);
    fetch(&alice, &[&alice, &server], Query::from("notes"));
    let sql = rusqlite::Connection::open(&path).unwrap();
    let stored = || {
        sql.query_row(
            "SELECT observed, content FROM _jazz_accepted_rows_v1",
            [],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
        )
        .unwrap()
    };
    let first = stored();
    assert_eq!(first.0, 1);
    assert_eq!(hex::encode(&first.1), format!("7f{}7f", "03".repeat(16)));
    let tx = begin(&alice);
    fetch(&alice, &[&alice, &server], Query::from("notes"));
    assert_eq!(stored(), first);
    assert_eq!(
        texts(&block_on(paired(&alice, tx, "notes", true, true, false)).unwrap()),
        ["accepted"]
    );
}

/// The host keeps the SDK's existing JSON/u32LE/row-codec ABI and registers a
/// read before returning its future, so Alice cannot race a raw binding commit.
#[test]
fn binding_pairs_metadata_and_rows_and_registers_before_polling() {
    use jazz::binding_host::Binding;
    use jazz::records::{OwnedRecord, RecordDescriptor, ValueType};
    let db = open(3, true);
    let binding = Binding::new(
        db,
        DbIdentity {
            node: NodeUuid::from_bytes([3; 16]),
            author: AuthorSubject::SYSTEM,
        },
        true,
    );
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let opts = serde_json::json!({"tier":"remote"});
    let tx = OpenTransactionId::new().to_string();
    binding
        .begin_transaction(&tx, "serializable", None, None)
        .unwrap();
    let empty = block_on(
        binding
            .all_settlement_metadata(query.clone(), &opts, &tx, None, None, true)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(empty, [2, 0, 0, 0, b'[', b']', 0]);
    binding.rollback_transaction(&tx).unwrap();
    let descriptor = RecordDescriptor::new([("text", ValueType::String)]);
    let record = descriptor
        .create(&[Value::String("accepted".into())])
        .unwrap();
    let cells =
        jazz::binding_codec::encode_named_cells(&OwnedRecord::new(record, descriptor)).unwrap();
    let write = binding
        .insert("notes", &cells, &serde_json::json!({}))
        .unwrap();
    binding
        .begin_transaction(&tx, "serializable", None, None)
        .unwrap();
    let metadata = block_on(
        binding
            .all_settlement_metadata(query.clone(), &opts, &tx, None, None, false)
            .unwrap(),
    )
    .unwrap();
    let frame = block_on(
        binding
            .all_settlement_metadata(query.clone(), &opts, &tx, None, None, true)
            .unwrap(),
    )
    .unwrap();
    let size = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
    assert_eq!(&frame[4..4 + size], metadata);
    let expected = block_on(binding.db.all(
        &binding.db.prepare_query(&Query::from("notes")).unwrap(),
        options(true, false),
    ))
    .unwrap();
    assert_eq!(
        frame[4 + size..],
        jazz::binding_codec::encode_rows(&expected).unwrap()
    );
    let metadata: serde_json::Value = serde_json::from_slice(&metadata).unwrap();
    assert_eq!(metadata[0]["rowId"], write.row_id.0.to_string());
    assert_eq!(metadata[0]["transactionId"], write.tx_id.to_string());
    assert_eq!(metadata[0]["position"], write.seq.to_string());
    binding
        .commit_transaction(&tx, Some("serializable"))
        .unwrap();
    binding
        .begin_transaction(&tx, "serializable", None, None)
        .unwrap();
    let unpolled = binding
        .all_settlement_metadata(query, &opts, &tx, None, None, true)
        .unwrap();
    assert!(
        binding
            .commit_transaction(&tx, Some("serializable"))
            .is_err()
    );
    assert!(block_on(unpolled).is_err());
}

/// Alice rolls back while an authority request is outstanding. The active
/// read must stop immediately, even if the authority never answers it.
#[test]
fn rollback_stops_an_active_network_read() {
    let server = open(3, true);
    let alice = open(1, false);
    connect(&alice, &server);
    let tx = begin(&alice);
    let mut read = pin!(paired(&alice, tx, "notes", false, false, false));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    alice.abandon_transaction_handle(tx).unwrap();
    let Poll::Ready(result) = read.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("read survived rollback")
    };
    assert_eq!(result.unwrap_err().code, ErrorCode::NotObserved);
}

/// Bob's replica transaction id differs from its authority position. Alice's
/// cold pair must preserve both rather than replacing Bob with the authority.
#[test]
fn uploaded_content_keeps_writer_identity_and_authority_position_distinct() {
    let server = open(3, true);
    let bob = open(2, false);
    connect(&bob, &server);
    let write = insert(&bob, "notes", "from Bob");
    drive(
        &[&bob, &server],
        bob.wait_for_transaction(write.mergeable_tx_id(), DurabilityTier::Global),
    )
    .unwrap();
    let alice = open(1, false);
    connect(&alice, &server);
    let tx = begin(&alice);
    let result = drive(
        &[&alice, &server],
        paired(&alice, tx, "notes", false, false, false),
    )
    .unwrap();
    assert_eq!(result.settlements[0].0.node, NodeUuid::from_bytes([2; 16]));
    assert_eq!(result.settlements[0].0.time.0, write.mergeable_tx_id());
    assert_ne!(result.settlements[0].1.0, write.mergeable_tx_id());
    assert!(result.settlements[0].1.0 < 1 << 52);
}

/// An ordinary relay answer is not authority coverage, even for an empty
/// result. A deliberately old peer response must refuse Alice's typed request.
#[test]
fn ordinary_cache_answer_cannot_satisfy_an_accepted_request() {
    use jazz::db::SyncMessage;
    let alice = open(1, false);
    let (a, mut b) = duplex();
    block_on(alice.connect_upstream(a));
    let tx = begin(&alice);
    let mut read = pin!(paired(&alice, tx, "notes", false, false, false));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    block_on(alice.tick()).unwrap();
    let id = std::iter::from_fn(|| b.try_recv())
        .find_map(|m| match m {
            SyncMessage::AcceptedFetchV1 { id, .. } => Some(id),
            _ => None,
        })
        .unwrap();
    b.send(SyncMessage::Answer {
        id,
        through: 7,
        rows: vec![],
    })
    .unwrap();
    block_on(alice.tick()).unwrap();
    assert_eq!(block_on(read).unwrap_err().code, ErrorCode::NotObserved);
    assert!(block_on(alice.commit_serializable_handle(tx)).is_err());
}
