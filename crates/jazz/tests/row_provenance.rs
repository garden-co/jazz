use std::collections::BTreeMap;

mod common;

use jazz::db::{Db, DbConfig, DbIdentity, MergeableTxOps, ReadOpts};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_writes, compile_schema};

fn author(byte: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([byte; 16])
}

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_writes()),
            )
            .build(),
    )
}

fn open_db(identity: AuthorSubject) -> Db<TestStorage> {
    let schema = schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = TestStorage::new(&refs);
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

    jazz::block_on(db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("first".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            updated_at_ms: Some(1_000),
            ..Default::default()
        },
    ))
    .expect("insert row");

    jazz::block_on(db.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
        jazz::db::UpdateOptions {
            updated_at_ms: Some(2_000),
            ..Default::default()
        },
    ))
    .expect("update row");

    let prepared = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = db.read(&prepared).expect("read row");
    assert_eq!(rows.len(), 1);

    let provenance = db
        .row_provenance(&rows[0])
        .expect("resolve provenance")
        .expect("row has provenance");
    // `Db::row_provenance` exposes physical Unix milliseconds, like every
    // public-facing provenance boundary. Packed HLC remains version-internal.
    let created_at = 1_000;
    let updated_at = 2_000;

    assert_eq!(provenance.created_by, alice);
    assert_eq!(provenance.created_at, created_at);
    assert_eq!(provenance.updated_by, alice);
    assert_eq!(provenance.updated_at, updated_at);
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
        created_at
    );
    assert_eq!(
        encoded.get_u64(updated_at_idx).expect("updatedAt value"),
        updated_at
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

    jazz::block_on(db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("first".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            updated_at_ms: Some(1_000),
            ..Default::default()
        },
    ))
    .expect("insert row");
    jazz::block_on(db.delete(
        "todos",
        row,
        jazz::db::DeleteOptions {
            updated_at_ms: Some(3_000),
            ..Default::default()
        },
    ))
    .expect("delete row");

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
    assert_eq!(provenance.created_at, 1_000);
    assert_eq!(provenance.updated_by, alice);
    assert_eq!(provenance.updated_at, 3_000);
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
    jazz::block_on(db.begin_mergeable(abandoned)).expect("open batch");
    db.abandon_transaction_handle(abandoned)
        .expect("abandon batch");
    let stale_error = jazz::block_on(db.mergeable_tx_ref(abandoned).update(
        "todos",
        row,
        BTreeMap::new(),
        Default::default(),
    ))
    .expect_err("stale batch handle must be rejected");
    assert!(stale_error.message.contains("open transaction"));

    let open = OpenTransactionId::new();
    jazz::block_on(db.begin_mergeable(open)).expect("open batch");
    let absent_error = jazz::block_on(db.mergeable_tx_ref(open).update(
        "todos",
        row,
        BTreeMap::new(),
        Default::default(),
    ))
    .expect_err("absent target must be rejected");
    assert!(
        absent_error.message.contains("must carry content cells"),
        "unexpected absent-target error: {}",
        absent_error.message
    );
    db.abandon_transaction_handle(open)
        .expect("abandon checked batch");
}
