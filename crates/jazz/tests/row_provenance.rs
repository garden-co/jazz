use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, MergeableTxOps, ReadOpts};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tools::OpenTransactionId;

fn author(byte: u8) -> AuthorId {
    AuthorId::from_bytes([byte; 16])
}

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)])
            .with_write_policy(Policy::shape(Query::from("todos"))),
    ])
}

fn open_db(identity: AuthorId) -> Db<MemoryStorage> {
    let schema = schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = MemoryStorage::new(&refs);
    jazz::db::block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: identity,
        },
        id_source: None,
    }))
    .expect("open db")
}

#[test]
fn row_provenance_preserves_created_fields_and_advances_updated_at() {
    let alice = author(0xa1);
    let row = RowUuid::from_bytes([0x33; 16]);
    let db = open_db(alice);

    db.insert_with_id_at_ms(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("first".to_owned()))]),
        1_000,
    )
    .expect("insert row");

    db.update_at_ms(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
        2_000,
    )
    .expect("update row");

    let prepared = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = db.read(&prepared).expect("read row");
    assert_eq!(rows.len(), 1);

    let provenance = db
        .row_provenance(&rows[0])
        .expect("resolve provenance")
        .expect("row has provenance");

    assert_eq!(provenance.created_by, alice);
    assert_eq!(provenance.created_at.0, 1_000);
    assert_eq!(provenance.updated_by, alice);
    assert_eq!(provenance.updated_at.0, 2_000);
    assert!(provenance.created_at < provenance.updated_at);

    let (descriptor, raw) = rows[0].encoded_record();
    let encoded = jazz::groove::records::BorrowedRecord::new(raw, descriptor);
    let created_at_idx = descriptor
        .field_index("$createdAt")
        .expect("createdAt field");
    let updated_at_idx = descriptor
        .field_index("$updatedAt")
        .expect("updatedAt field");
    assert_eq!(
        encoded.get_u64(created_at_idx).expect("createdAt value"),
        1_000
    );
    assert_eq!(
        encoded.get_u64(updated_at_idx).expect("updatedAt value"),
        2_000
    );
}

/// A soft deletion keeps Alice's original creation provenance while exposing
/// the deletion event as the row's latest update on an include-deleted read.
///
/// alice ──insert(t=1000)──► row ──delete(t=3000)──► include-deleted read
#[test]
fn deletion_advances_updated_provenance_without_replacing_creation_provenance() {
    let alice = author(0xa1);
    let row = RowUuid::from_bytes([0x44; 16]);
    let db = open_db(alice);

    db.insert_with_id_at_ms(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("first".to_owned()))]),
        1_000,
    )
    .expect("insert row");
    db.delete_at_ms("todos", row, 3_000).expect("delete row");

    let prepared = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = jazz::block_on(db.all(
        &prepared,
        ReadOpts {
            include_deleted: true,
            ..ReadOpts::default()
        },
    ))
    .expect("read deleted row");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].is_deleted());

    let provenance = db
        .row_provenance(&rows[0])
        .expect("resolve provenance")
        .expect("row has provenance");
    assert_eq!(provenance.created_by, alice);
    assert_eq!(provenance.created_at.0, 1_000);
    assert_eq!(provenance.updated_by, alice);
    assert_eq!(provenance.updated_at.0, 3_000);
}

/// Empty mergeable-batch updates remain no-ops only after validating both the
/// open transaction handle and the target row; they cannot turn a stale handle
/// or absent target into a silently accepted operation.
///
/// alice ──abandon(batch)──► empty update ✗
/// alice ──open(batch)──► empty update(absent row) ✗
#[test]
fn empty_batched_update_still_validates_handle_and_target() {
    let db = open_db(author(0xa1));
    let row = RowUuid::from_bytes([0x55; 16]);

    let abandoned = OpenTransactionId::new();
    db.begin_mergeable(abandoned).expect("open batch");
    db.abandon_transaction_handle(abandoned)
        .expect("abandon batch");
    let stale_error = db
        .mergeable_tx_ref(abandoned)
        .update("todos", row, BTreeMap::new())
        .expect_err("stale batch handle must be rejected");
    assert!(stale_error.message.contains("open transaction"));

    let open = OpenTransactionId::new();
    db.begin_mergeable(open).expect("open batch");
    let absent_error = db
        .mergeable_tx_ref(open)
        .update("todos", row, BTreeMap::new())
        .expect_err("absent target must be rejected");
    assert!(
        absent_error.message.contains("must carry content cells"),
        "unexpected absent-target error: {}",
        absent_error.message
    );
    db.abandon_transaction_handle(open)
        .expect("abandon checked batch");
}
