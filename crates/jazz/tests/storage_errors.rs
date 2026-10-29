//! A storage error while recording sync state fails the operation that
//! needed it instead of being swallowed: a write whose upload record cannot
//! be stored is rolled back whole, a transaction whose rollback finds its
//! savepoint gone still forgets its changes, and an authority does not
//! validate a serializable commit against an unreadable store. The errors are injected by hiding
//! one of the store's own tables through a second connection. A reopened
//! replica also resumes its subscriptions from the watermarks its file
//! keeps.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, ReadSpan, SyncMessage,
    Transport, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz::wire::TransportError;

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("label", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn config(node: u8) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn scratch(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("jazz-storage-errors-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

/// A replica: its commits are owed to an upstream, so each one records
/// its upload.
fn open_file(path: &Path, node: u8) -> Db {
    let db = block_on(Db::open_at(config(node), path, FileOptions::default())).unwrap();
    db.become_replica();
    db
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn item(label: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("label".to_owned(), Value::String(label.to_owned()))])
}

fn insert(db: &Db, n: u8, label: &str) -> Result<u64, jazz::db::Error> {
    block_on(db.insert(
        "items",
        item(label),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .map(|handle| handle.mergeable_tx_id())
}

fn labels(db: &Db) -> Vec<String> {
    let prepared = db.prepare_query(&Query::from("items")).unwrap();
    let mut labels: Vec<String> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .filter_map(|row| match row.cells().find(|(name, _)| *name == "label") {
            Some((_, Some(Value::String(label)))) => Some(label.clone()),
            _ => None,
        })
        .collect();
    labels.sort();
    labels
}

/// Hide one of the store's tables from the open database (and bring it
/// back), through a second connection to its file.
fn hide(path: &Path, table: &str) {
    rusqlite::Connection::open(path)
        .unwrap()
        .execute_batch(&format!("ALTER TABLE {table} RENAME TO {table}_hidden"))
        .unwrap();
}

fn restore(path: &Path, table: &str) {
    rusqlite::Connection::open(path)
        .unwrap()
        .execute_batch(&format!("ALTER TABLE {table}_hidden RENAME TO {table}"))
        .unwrap();
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..100 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

/// Sync the replica's commits to a fresh server and return its rows.
fn synced(client: &Db, txs: &[u64]) -> Vec<String> {
    let server = block_on(Db::open_history_complete(config(0xee))).unwrap();
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    for tx in txs {
        drive(
            &[client, &server, client],
            client.wait_for_transaction(*tx, DurabilityTier::Global),
        )
        .unwrap();
    }
    labels(&server)
}

/// The tables a pending upload is stored in (SPEC 8.3): its header, its
/// ops and lists, and the local rows it wrote.
const UPLOAD_TABLES: [&str; 3] = ["_jazz_pending", "_jazz_pending_items", "_jazz_pending_rows"];

#[test]
fn a_write_whose_upload_record_cannot_be_stored_is_rolled_back_whole() {
    for table in UPLOAD_TABLES {
        let path = scratch(&format!("upload-write{table}"));
        let client = open_file(&path, 3);
        let first = insert(&client, 1, "kept").unwrap();

        hide(&path, table);
        let failed = insert(&client, 2, "lost");
        restore(&path, table);
        assert!(
            failed.is_err(),
            "{table}: the write reports the storage error"
        );
        assert_eq!(
            labels(&client),
            ["kept"],
            "{table}: and leaves no row behind"
        );

        // The replica carries on as if the failed write never happened:
        // the same row id inserts again (under the same seq, so no part of
        // the failed upload record may remain), and both commits reach the
        // server once.
        let second = insert(&client, 2, "retried").unwrap();
        assert_eq!(labels(&client), ["kept", "retried"]);
        assert_eq!(synced(&client, &[first, second]), ["kept", "retried"]);
    }
}

#[test]
fn a_transaction_whose_upload_record_cannot_be_stored_is_rolled_back_whole() {
    for table in UPLOAD_TABLES {
        let path = scratch(&format!("upload-transaction{table}"));
        let client = open_file(&path, 3);

        hide(&path, table);
        let failed = block_on(client.transaction(async |tx| {
            for (n, label) in [(1, "a"), (2, "b")] {
                tx.insert(
                    "items",
                    item(label),
                    InsertOptions {
                        row_id: Some(row(n)),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        }));
        restore(&path, table);
        assert!(
            failed.is_err(),
            "{table}: the transaction reports the storage error"
        );
        assert!(
            labels(&client).is_empty(),
            "{table}: and none of its rows remain"
        );

        let (_, receipt) = block_on(client.transaction(async |tx| {
            tx.insert(
                "items",
                item("c"),
                InsertOptions {
                    row_id: Some(row(1)),
                    ..Default::default()
                },
            )
            .await
        }))
        .unwrap();
        assert_eq!(labels(&client), ["c"]);
        assert_eq!(synced(&client, &[receipt.0]), ["c"]);
    }
}

#[test]
fn a_transaction_rolled_back_by_sqlite_itself_reports_its_own_error() {
    let path = scratch("rolled-back");
    let client = open_file(&path, 3);

    let failed: Result<((), _), _> = block_on(client.transaction(async |tx| {
        tx.insert(
            "items",
            item("gone"),
            InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        )
        .await?;
        // What SQLite does on a full disk or an I/O error mid-transaction:
        // the whole transaction is rolled back, savepoints and all.
        client.rollback_sqlite_transaction_for_test();
        Err(jazz::db::Error::new(
            jazz::db::ErrorCode::Storage,
            "disk I/O error".to_owned(),
        ))
    }));
    let error = failed.unwrap_err().to_string();
    assert!(
        error.contains("disk I/O error"),
        "the transaction reports its own error, not the missing savepoint: {error}"
    );
    assert!(labels(&client).is_empty());

    let tx = insert(&client, 2, "next").unwrap();
    assert_eq!(labels(&client), ["next"]);
    assert_eq!(synced(&client, &[tx]), ["next"]);
}

/// A transport that keeps every message it was given.
struct Recording {
    inner: Box<dyn Transport>,
    sent: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for Recording {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.sent.borrow_mut().push_back(message.clone());
        self.inner.send(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }

    fn take_failure(&mut self) -> Option<TransportError> {
        self.inner.take_failure()
    }
}

/// A reopened replica resumes each subscription from the seq its result
/// was last settled through, which it keeps in its file: a reconnect
/// declares no held rows.
#[test]
fn a_reopened_replica_resumes_its_subscription_from_its_stored_watermark() {
    let path = scratch("watermark");
    let server = block_on(Db::open_history_complete(config(0xee))).unwrap();
    let client = open_file(&path, 3);
    let tx = insert(&client, 1, "held").unwrap();
    let (client_side, server_side) = duplex();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    let prepared = client.prepare_query(&Query::from("items")).unwrap();
    let stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    drive(
        &[&client, &server, &client],
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    for _ in 0..3 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
    assert!(client.detach_connection(&upstream));
    drop(stream);
    drop(client);

    let client = open_file(&path, 3);
    let (client_side, server_side) = duplex();
    let sent = Rc::new(RefCell::new(VecDeque::new()));
    let _upstream = block_on(client.connect_upstream(Box::new(Recording {
        inner: client_side,
        sent: Rc::clone(&sent),
    })));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    let prepared = client.prepare_query(&Query::from("items")).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    block_on(client.tick()).unwrap();
    let sent: Vec<SyncMessage> = sent.borrow_mut().drain(..).collect();
    assert!(
        !sent.iter().any(|m| matches!(m, SyncMessage::Hold { .. })),
        "no held rows are declared: {sent:?}"
    );
    let since = sent.iter().find_map(|m| match m {
        SyncMessage::Read {
            span: ReadSpan::Live { since },
            ..
        } => Some(*since),
        _ => None,
    });
    assert!(
        matches!(since, Some(Some(seq)) if seq > 0),
        "the subscription resumes from its stored watermark: {since:?}"
    );
    assert_eq!(labels(&client), ["held"]);
}

/// #3070: an authority that cannot read the rows a serializable commit read
/// does not accept the commit as if they were absent. Here the commit read
/// that no item exists, and one was added since: it is refused with the
/// storage error, never accepted.
#[test]
fn a_serializable_commit_is_not_validated_against_an_unreadable_store() {
    let path = scratch("serializable-validation");
    let server = block_on(Db::open_at(config(0xee), &path, FileOptions::default())).unwrap();
    server.set_authority();
    let client = block_on(Db::open(config(0x11))).unwrap();
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);

    let open = jazz::tools::OpenTransactionId::new();
    block_on(client.begin_serializable(open)).unwrap();
    let prepared = client.prepare_query(&Query::from("items")).unwrap();
    let read = drive(
        &[&client, &server, &client],
        client.tx_ref(open).all_prepared(&prepared),
    );
    assert_eq!(read.unwrap().len(), 0);
    block_on(client.tx_ref(open).insert(
        "items",
        item("guarded"),
        InsertOptions {
            row_id: Some(row(2)),
            ..Default::default()
        },
    ))
    .unwrap();
    // Someone else adds an item; the commit's read no longer holds.
    block_on(server.insert(
        "items",
        item("guard"),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let receipt = block_on(client.commit_serializable_handle(open)).unwrap();

    let stored: String = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '%items%' \
             ORDER BY length(name) LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    hide(&path, &stored);
    for _ in 0..6 {
        let _ = block_on(client.tick());
        let _ = block_on(server.tick());
    }
    restore(&path, &stored);
    let outcome = drive(
        &[&client, &server, &client],
        client.wait_for_transaction(receipt.0, DurabilityTier::Global),
    );
    let error = outcome.expect_err("an unreadable read set is not accepted");
    assert!(error.message.contains("Storage"), "{error:?}");
    assert_eq!(labels(&server), ["guard"]);
}
