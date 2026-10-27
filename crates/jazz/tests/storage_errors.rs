//! A storage error while recording sync state fails the operation that
//! needed it instead of being swallowed: a write whose upload record cannot
//! be stored is rolled back whole, a transaction whose rollback finds its
//! savepoint gone still forgets its changes, and a new upstream session
//! whose held rows cannot be read reports it and declares them on the next
//! send. The errors are injected by hiding one of the store's own tables
//! through a second connection.

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
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, SyncMessage, Transport,
    block_on,
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

#[test]
fn a_write_whose_upload_record_cannot_be_stored_is_rolled_back_whole() {
    let path = scratch("upload-write");
    let client = open_file(&path, 3);
    let first = insert(&client, 1, "kept").unwrap();

    hide(&path, "_jazz_pending");
    let failed = insert(&client, 2, "lost");
    restore(&path, "_jazz_pending");
    assert!(failed.is_err(), "the write reports the storage error");
    assert_eq!(labels(&client), ["kept"], "and leaves no row behind");

    // The replica carries on as if the failed write never happened: the
    // same row id inserts again, and both commits reach the server once.
    let second = insert(&client, 2, "retried").unwrap();
    assert_eq!(labels(&client), ["kept", "retried"]);
    assert_eq!(synced(&client, &[first, second]), ["kept", "retried"]);
}

#[test]
fn a_transaction_whose_upload_record_cannot_be_stored_is_rolled_back_whole() {
    let path = scratch("upload-transaction");
    let client = open_file(&path, 3);

    hide(&path, "_jazz_pending");
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
    restore(&path, "_jazz_pending");
    assert!(failed.is_err(), "the transaction reports the storage error");
    assert!(labels(&client).is_empty(), "and none of its rows remain");

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

#[test]
fn held_rows_that_cannot_be_read_fail_the_send_and_are_declared_on_the_next() {
    let path = scratch("hold");
    let server = block_on(Db::open_history_complete(config(0xee))).unwrap();
    let client = open_file(&path, 3);
    let tx = insert(&client, 1, "held").unwrap();
    let (client_side, server_side) = duplex();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    drive(
        &[&client, &server, &client],
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    assert!(client.detach_connection(&upstream));

    let (client_side, server_side) = duplex();
    let sent = Rc::new(RefCell::new(VecDeque::new()));
    let _upstream = block_on(client.connect_upstream(Box::new(Recording {
        inner: client_side,
        sent: Rc::clone(&sent),
    })));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);

    hide(&path, "items");
    let failed = block_on(client.tick());
    restore(&path, "items");
    assert!(failed.is_err(), "the send reports the storage error");
    assert!(
        sent.borrow().is_empty(),
        "and sends nothing without the hold"
    );

    block_on(client.tick()).unwrap();
    let sent: Vec<SyncMessage> = sent.borrow_mut().drain(..).collect();
    let kinds: Vec<&str> = sent
        .iter()
        .map(|message| match message {
            SyncMessage::Schema { .. } => "schema",
            SyncMessage::Hold { .. } => "hold",
            _ => "other",
        })
        .collect();
    assert_eq!(kinds.first(), Some(&"schema"), "{kinds:?}");
    assert_eq!(kinds.get(1), Some(&"hold"), "the hold follows the schema");
    let SyncMessage::Hold { rows } = &sent[1] else {
        unreachable!()
    };
    assert_eq!(rows.len(), 1, "the settled row is declared: {rows:?}");
    assert_eq!(rows[0].1, *row(1).0.as_bytes());
}
