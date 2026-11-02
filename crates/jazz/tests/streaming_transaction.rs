//! Streamed large values staged in an open transaction: the batch holds the
//! upload's chunks until it commits or is abandoned.

use std::collections::BTreeMap;

use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{
    Db, DbConfig, DbIdentity, LargeValueStagingPolicy, ReadOpts, StreamingMutationKind,
    WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::large_values::LEAF_MAX_BYTES;
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

fn open() -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("files")
                    .nullable_column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x31; 16]),
            author: AuthorSubject::for_test_bytes([0x41; 16]),
        },
    )))
    .unwrap()
}

fn text(value: Value) -> String {
    match value {
        Value::Nullable(Some(value)) => text(*value),
        Value::String(value) => value,
        other => panic!("expected text, got {other:?}"),
    }
}

fn bodies(rows: Vec<jazz::db::CurrentRow>) -> Vec<String> {
    rows.iter()
        .map(|row| {
            text(
                row.cells()
                    .find(|(name, _)| *name == "body")
                    .unwrap()
                    .1
                    .cloned()
                    .unwrap(),
            )
        })
        .collect()
}

fn committed(db: &Db) -> Vec<String> {
    let query = db.prepare_query(&Query::from("files")).unwrap();
    bodies(block_on(db.all(&query, ReadOpts::default())).unwrap())
}

fn staged(db: &Db, tx: OpenTransactionId) -> Vec<String> {
    let query = db.prepare_query(&Query::from("files")).unwrap();
    bodies(block_on(db.tx_ref(tx).all_prepared(&query)).unwrap())
}

/// A value large enough to be stored as chunks, not inline.
fn large() -> String {
    "streamed ".repeat(LEAF_MAX_BYTES / 3)
}

/// Stream `body` into a new row staged in `tx`.
fn stream_in(db: &Db, tx: OpenTransactionId, row: u8, body: &str) {
    let mut upload = db
        .begin_streaming_value_upload("files", &BTreeMap::new(), "body")
        .unwrap();
    for chunk in body.as_bytes().chunks(10_000) {
        block_on(db.push_streaming_value_upload(&mut upload, chunk)).unwrap();
    }
    block_on(db.tx_ref(tx).finish_streaming_value_upload(
        upload,
        StreamingMutationKind::Insert,
        "files",
        RowUuid::from_bytes([row; 16]),
        BTreeMap::new(),
        "body",
        WriteIdentity::Database,
        None,
        None,
        None,
    ))
    .unwrap();
}

fn expire_uploads_now(db: &Db) -> usize {
    db.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 60_000,
        max_age_ms: 0,
    });
    std::thread::sleep(std::time::Duration::from_millis(2));
    block_on(db.evict_expired_staged_large_values()).unwrap()
}

#[test]
fn a_streamed_value_commits_with_its_transaction() {
    let db = open();
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    let body = large();
    stream_in(&db, tx, 1, &body);
    stream_in(&db, tx, 2, "small");

    assert!(committed(&db).is_empty());
    let mut inside = staged(&db, tx);
    inside.sort();
    assert_eq!(inside, ["small".to_owned(), body.clone()]);

    // Maintenance while the transaction is open keeps the staged chunks.
    for _ in 0..4 {
        block_on(db.tick()).unwrap();
    }
    block_on(db.commit_mergeable_handle(tx)).unwrap();
    for _ in 0..4 {
        block_on(db.tick()).unwrap();
    }
    let mut rows = committed(&db);
    rows.sort();
    assert_eq!(rows, ["small".to_owned(), body]);
}

#[test]
fn a_serializable_transaction_commits_a_streamed_value() {
    let db = open();
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    let body = large();
    stream_in(&db, tx, 1, &body);
    block_on(db.commit_serializable_handle(tx)).unwrap();
    assert_eq!(committed(&db), [body]);
}

#[test]
fn an_abandoned_transaction_releases_its_streamed_chunks() {
    let db = open();
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    stream_in(&db, tx, 1, &large());
    db.abandon_transaction_handle(tx).unwrap();
    block_on(db.tick()).unwrap();
    assert!(committed(&db).is_empty());
    // The lease went with the batch: nothing is left to expire.
    assert_eq!(expire_uploads_now(&db), 0);
}

#[test]
fn an_open_transaction_holds_its_streamed_lease() {
    let db = open();
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    stream_in(&db, tx, 1, &large());
    block_on(db.tick()).unwrap();
    assert_eq!(expire_uploads_now(&db), 1);
    // Its chunks may be gone, so the commit is refused.
    let error = block_on(db.commit_mergeable_handle(tx)).unwrap_err();
    assert!(error.to_string().contains("expired"), "{error}");
    assert!(committed(&db).is_empty());
}
