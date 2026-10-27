//! Read-only commits must reach authority validation and retain unique durable ids.
mod common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, RemoteLinkHint, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz::tx::{DurabilityTier, Fate};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

fn author(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}
fn open(node: u8, path: Option<&Path>, scoped: bool) -> Db {
    open_as(node, path, scoped, AuthorSubject::SYSTEM)
}
fn open_as(node: u8, path: Option<&Path>, scoped: bool, author: AuthorSubject) -> Db {
    let policies = if scoped {
        TablePolicies::new()
            .with_select(PolicyExpr::eq_session(
                "owner",
                vec!["claims".into(), "reader".into()],
            ))
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
    } else {
        common::allow_all_policies()
    };
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    );
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap()
}
fn connect(client: &Db, server: &Db, identity: AuthorSubject) {
    let (a, b) = duplex_transport::duplex();
    block_on(client.connect_upstream(a));
    server.accept_subscriber(b, identity);
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..150 {
        if let Poll::Ready(result) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return result;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("transaction did not settle");
}
fn insert(db: &Db, owner: &str) -> jazz::db::WriteHandle {
    block_on(db.insert(
        "notes",
        BTreeMap::from([("owner".into(), Value::String(owner.into()))]),
        Default::default(),
    ))
    .unwrap()
}
fn read(db: &Db, tx: OpenTransactionId, dbs: &[&Db]) -> usize {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    drive(dbs, db.tx_ref(tx).all_prepared(&query))
        .unwrap()
        .len()
}
fn empty(db: &Db, serializable: bool) -> u64 {
    let tx = OpenTransactionId::new();
    if serializable {
        block_on(db.begin_serializable(tx)).unwrap();
        block_on(db.commit_serializable_handle(tx)).unwrap().0
    } else {
        block_on(db.begin_mergeable(tx)).unwrap();
        block_on(db.commit_mergeable_handle(tx)).unwrap().0
    }
}

/// Alice reads without writing. Global acceptance validates her original rows;
/// a later update or phantom insertion must reject the read-only transaction.
#[test]
fn read_only_commits_validate_rows_and_empty_query_phantoms() {
    for had_row in [false, true] {
        let server = open(3, None, false);
        server.set_authority();
        let previous = had_row.then(|| insert(&server, "alice"));
        let client = open(1, None, false);
        connect(&client, &server, AuthorSubject::SYSTEM);
        let tx = OpenTransactionId::new();
        block_on(client.begin_serializable(tx)).unwrap();
        assert_eq!(
            read(&client, tx, &[&client, &server, &client]),
            usize::from(had_row)
        );
        if let Some(previous) = previous {
            block_on(server.update(
                "notes",
                previous.row_uuid(),
                BTreeMap::from([("owner".into(), Value::String("bob".into()))]),
                Default::default(),
            ))
            .unwrap();
        } else {
            insert(&server, "bob");
        }
        // No client tick: authority validation must catch the change itself.
        let commit = block_on(client.commit_serializable_handle(tx)).unwrap().0;
        let error = drive(
            &[&client, &server, &client],
            client.wait_for_transaction(commit, DurabilityTier::Global),
        )
        .unwrap_err();
        assert!(error.is_serialization_conflict(), "{error}");
        assert!(error.is_conflict(), "authority conflicts remain retryable");
        assert!(matches!(
            client.write_state(commit).unwrap().fate,
            Fate::Rejected(_)
        ));
    }
}

/// Alice's unchanged read set reaches the authority and gets a real receipt.
/// Two consecutive read-only transactions must never share an upload id.
#[test]
fn unchanged_reads_receive_distinct_authority_receipts() {
    let server = open(3, None, false);
    server.set_authority();
    insert(&server, "alice");
    let client = open(1, None, false);
    connect(&client, &server, AuthorSubject::SYSTEM);
    let mut ids = vec![];
    for _ in 0..2 {
        let tx = OpenTransactionId::new();
        block_on(client.begin_serializable(tx)).unwrap();
        assert_eq!(read(&client, tx, &[&client, &server, &client]), 1);
        let id = block_on(client.commit_serializable_handle(tx)).unwrap().0;
        assert_eq!(client.write_state(id).unwrap().fate, Fate::Pending);
        drive(
            &[&client, &server, &client],
            client.wait_for_transaction(id, DurabilityTier::Global),
        )
        .unwrap();
        assert_eq!(client.write_state(id).unwrap().fate, Fate::Accepted);
        ids.push((id, client.write_state(id).unwrap().global_time.unwrap().0));
    }
    assert!(ids[1].0 > ids[0].0 && ids[1].1 > ids[0].1);
}

/// Alice's empty standalone transactions have durable, distinct receipts even
/// though no row can retain their sequence. Reopening must not reuse any id.
#[test]
fn empty_local_transactions_keep_their_sequence_across_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("owner.sqlite");
    let last = {
        let db = open(1, Some(&path), false);
        db.set_authority();
        let first = empty(&db, false);
        let second = empty(&db, true);
        let (_, receipt) = block_on(db.transaction(async |_tx| Ok(()))).unwrap();
        assert!(first < second && second < receipt.0);
        for id in [first, second, receipt.0] {
            block_on(db.wait_for_transaction(id, DurabilityTier::Global)).unwrap();
        }
        receipt.0
    };
    let db = open(1, Some(&path), false);
    db.set_authority();
    assert!(empty(&db, true) > last);
    block_on(db.wait_for_transaction(last, DurabilityTier::Global)).unwrap();
}

/// Alice's offline read-only commits survive a replica restart, are validated
/// after reconnect, and retain authority ordering when both stores reopen.
#[test]
fn pending_and_accepted_empty_commits_survive_both_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let cp = dir.path().join("client.sqlite");
    let sp = dir.path().join("server.sqlite");
    let first = {
        let db = open(1, Some(&cp), false);
        db.set_remote_link_hint(RemoteLinkHint::Failed);
        let tx = OpenTransactionId::new();
        block_on(db.begin_serializable(tx)).unwrap();
        assert_eq!(read(&db, tx, &[]), 0);
        block_on(db.commit_serializable_handle(tx)).unwrap().0
    };
    let (second, accepted) = {
        let client = open(1, Some(&cp), false);
        client.set_remote_link_hint(RemoteLinkHint::Failed);
        let second = empty(&client, true);
        assert!(second > first);
        let server = open(3, Some(&sp), false);
        server.set_authority();
        connect(&client, &server, AuthorSubject::SYSTEM);
        for id in [first, second] {
            drive(
                &[&client, &server, &client],
                client.wait_for_transaction(id, DurabilityTier::Global),
            )
            .unwrap();
        }
        (
            second,
            client.write_state(second).unwrap().global_time.unwrap().0,
        )
    };
    let client = open(1, Some(&cp), false);
    client.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(
        client.write_state(second).unwrap().global_time.unwrap().0,
        accepted
    );
    let next = empty(&client, true);
    assert!(next > second);
    let server = open(3, Some(&sp), false);
    server.set_authority();
    connect(&client, &server, AuthorSubject::SYSTEM);
    drive(
        &[&client, &server, &client],
        client.wait_for_transaction(next, DurabilityTier::Global),
    )
    .unwrap();
    assert!(client.write_state(next).unwrap().global_time.unwrap().0 > accepted);
}

/// Alice's bound session and claims must travel even without mutation ops.
/// Otherwise a trusted SYSTEM peer would validate her read against Bob's rows.
#[test]
fn read_only_session_batches_preserve_identity_and_claims() {
    let server = open(3, None, true);
    server.set_authority();
    insert(&server, "alice");
    insert(&server, "bob");
    let client = open(1, None, true);
    connect(&client, &server, AuthorSubject::SYSTEM);
    let alice = author("alice");
    client.set_identity_claims(
        alice,
        BTreeMap::from([("\0claims:reader".into(), Value::String("alice".into()))]),
    );
    let tx = OpenTransactionId::new();
    block_on(client.begin_serializable_for_identity(tx, alice)).unwrap();
    assert_eq!(read(&client, tx, &[&client, &server, &client]), 1);
    let id = block_on(client.commit_serializable_handle(tx)).unwrap().0;
    drive(
        &[&client, &server, &client],
        client.wait_for_transaction(id, DurabilityTier::Global),
    )
    .unwrap();
}

/// Alice's ordinary session connection governs even an empty batch naming
/// Mallory. Both commit kinds settle normally under the connection's identity.
#[test]
fn empty_session_batches_follow_the_ordinary_connection_identity() {
    for serializable in [false, true] {
        let server = open(3, None, false);
        server.set_authority();
        let client = open(1, None, false);
        connect(&client, &server, author("alice"));
        let tx = OpenTransactionId::new();
        let id = if serializable {
            block_on(client.begin_serializable_for_identity(tx, author("mallory"))).unwrap();
            block_on(client.commit_serializable_handle(tx)).unwrap().0
        } else {
            block_on(client.begin_mergeable_for_identity(tx, author("mallory"))).unwrap();
            block_on(client.commit_mergeable_handle(tx)).unwrap().0
        };
        drive(
            &[&client, &server, &client],
            client.wait_for_transaction(id, DurabilityTier::Global),
        )
        .unwrap();
        assert_eq!(client.write_state(id).unwrap().fate, Fate::Accepted);
    }
}

/// Lower-level SQL inspection pins the new durable allocator bytes; rows cannot
/// prove an empty transaction's persisted floor. Reopens use public Db::open_at.
#[test]
fn issued_sequence_v1_has_exact_persisted_bytes() {
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_issued_sequence_v1.json")).unwrap();
    assert_eq!(corpus["role"], "jazz.sqlite.issued-sequence.v1");
    assert_eq!(corpus["version"], 1);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("floor.sqlite");
    for case in corpus["cases"].as_array().unwrap() {
        let value = case["value"].as_str().unwrap().parse::<u64>().unwrap();
        let bytes = postcard::to_allocvec(&value).unwrap();
        assert_eq!(
            bytes.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            case["hex"].as_str().unwrap()
        );
        if value == 0 {
            continue;
        }
        {
            let db = open(1, Some(&path), false);
            db.seed_seq_floor(value - 1);
            assert_eq!(empty(&db, true), value);
        }
        let sql = rusqlite::Connection::open(&path).unwrap();
        let actual: Vec<u8> = sql
            .query_row(
                "SELECT v FROM _jazz_meta WHERE k='issued-seq-v1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(actual, bytes);
        drop(sql);
        assert_eq!(open(1, Some(&path), false).seq_high_water(), value);
    }
}

/// Direct corruption models an unreadable allocator record. Reopening must
/// fail instead of allocating an id that may name Alice's earlier transaction.
#[test]
fn invalid_allocator_records_fail_open_explicitly() {
    for bytes in [
        vec![255],
        vec![1, 0],
        postcard::to_allocvec(&u64::MAX).unwrap(),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("floor.sqlite");
        {
            let db = open(1, Some(&path), false);
            empty(&db, true);
        }
        let sql = rusqlite::Connection::open(&path).unwrap();
        sql.execute(
            "UPDATE _jazz_meta SET v=?1 WHERE k='issued-seq-v1'",
            [bytes],
        )
        .unwrap();
        drop(sql);
        // The helper deliberately panics on open errors; inspect the actual
        // public error using an equivalent public schema/config instead.
        let schema = common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("owner", ColumnType::Text)
                        .policies(common::allow_all_policies()),
                )
                .build(),
        );
        let error = block_on(Db::open_at(
            DbConfig::new(
                schema,
                DbIdentity {
                    node: NodeUuid::from_bytes([1; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            ),
            &path,
            Default::default(),
        ))
        .err()
        .expect("corrupt floor must fail");
        assert_eq!(error.code, ErrorCode::Storage);
        assert!(error.message.contains("issued-sequence-v1"));
    }
}

/// This lower-level wire fixture freezes the existing Commit envelope with an
/// empty mutation list; it must still carry serializable snapshot/read fields.
#[test]
fn read_only_commits_keep_the_existing_wire_layout() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v3/readonly-commit.json"
    ))
    .unwrap();
    assert_eq!(fixture["role"], "jazz.sqlite.sync.read-only-commit.v1");
    assert_eq!(fixture["version"], 3);
    let message: jazz::db::SyncMessage =
        serde_json::from_value(fixture["message"].clone()).unwrap();
    let bytes = jazz::db::encode_message(&message).unwrap();
    assert_eq!(
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        fixture["hex"].as_str().unwrap()
    );
    let decoded = jazz::db::decode_message(&bytes).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), fixture["message"]);
}

/// Alice's foreground can read its owner, but an empty commit still needs relay
/// admission and durable acknowledgement. It must not fabricate a receipt.
#[test]
fn foreground_empty_commits_remain_explicitly_unsupported() {
    let db = open_as(1, None, false, author("alice"));
    db.set_private_owner_client().unwrap();
    for serializable in [false, true] {
        let tx = OpenTransactionId::new();
        let result = if serializable {
            block_on(db.begin_serializable(tx)).unwrap();
            block_on(db.commit_serializable_handle(tx))
        } else {
            block_on(db.begin_mergeable(tx)).unwrap();
            block_on(db.commit_mergeable_handle(tx))
        };
        assert!(
            result
                .unwrap_err()
                .message
                .contains("non-durable foreground")
        );
    }
    assert!(
        block_on(db.transaction(async |_tx| Ok(())))
            .unwrap_err()
            .message
            .contains("non-durable foreground")
    );
}

/// Deliberate SQLite failure injection checks the atomic boundary: Alice's row
/// and its allocator floor either both commit or both roll back, for each API.
#[test]
fn allocator_write_failure_rolls_back_rows_and_receipts() {
    for api in ["direct", "batch", "callback"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("failure.sqlite");
        let db = open(1, Some(&path), false);
        let sql = rusqlite::Connection::open(&path).unwrap();
        sql.execute_batch("CREATE TRIGGER fail_floor BEFORE INSERT ON _jazz_meta WHEN NEW.k='issued-seq-v1' BEGIN SELECT RAISE(ABORT,'injected allocator storage failure'); END;").unwrap();
        let cells = || BTreeMap::from([("owner".into(), Value::String("alice".into()))]);
        let error = match api {
            "direct" => block_on(db.insert("notes", cells(), Default::default())).unwrap_err(),
            "batch" => {
                let tx = OpenTransactionId::new();
                block_on(db.begin_mergeable(tx)).unwrap();
                block_on(db.tx_ref(tx).insert("notes", cells(), Default::default())).unwrap();
                block_on(db.commit_mergeable_handle(tx)).unwrap_err()
            }
            _ => block_on(db.transaction(async |tx| {
                tx.insert("notes", cells(), Default::default()).await?;
                Ok(())
            }))
            .unwrap_err(),
        };
        assert_eq!(error.code, ErrorCode::Storage);
        assert_eq!(db.seq_high_water(), 0);
        let query = db.prepare_query(&Query::from("notes")).unwrap();
        assert!(
            block_on(db.all(&query, Default::default()))
                .unwrap()
                .is_empty()
        );
        sql.execute_batch("DROP TRIGGER fail_floor").unwrap();
        drop(sql);
        assert_eq!(insert(&db, "alice").mergeable_tx_id(), 1);
    }
}
