//! Transactional writes on rows whose other columns hold large values (#3507).
//!
//! A large text/bytes cell is stored physically as an engine-owned indirect
//! descriptor. Updating an unrelated small column inside `db.transaction`
//! must carry that untouched descriptor forward exactly as the standalone
//! `db.update` path does, while a caller-authored descriptor stays rejected.
//!
//! All tests use one in-process `Db` runtime: the contract is local write
//! lowering, so no server topology is needed.
use std::collections::BTreeMap;

mod common;

use common::{allow_all_policies, compile_schema};
use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, ExclusiveTxOps, InsertOptions, MergeableTxOps, UpdateOptions,
    UpsertOptions,
};
use jazz::groove::large_values::{
    ContentHash, FORMAT_VERSION, INLINE_VALUE_MAX_BYTES, LargeValueKind, LargeValueRef, Locator,
    NodeRef,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

const LARGE_BYTES: usize = 200 * 1024;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .column("rank", ColumnType::Integer)
                    .column("body", ColumnType::Text)
                    .column("blob", ColumnType::Bytea)
                    .nullable_column("note", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open_db() -> Db<TestStorage> {
    let schema = schema();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema,
        TestStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0x35; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open db")
}

fn big_text(fill: char) -> String {
    assert!(LARGE_BYTES > INLINE_VALUE_MAX_BYTES);
    std::iter::repeat_n(fill, LARGE_BYTES).collect()
}

fn big_bytes(fill: u8) -> Vec<u8> {
    vec![fill; LARGE_BYTES]
}

fn some(value: Value) -> Value {
    Value::Nullable(Some(Box::new(value)))
}

/// Seed one row with 200 KB text, 200 KB bytes and a 200 KB nullable text.
fn seed(db: &Db<TestStorage>) -> RowUuid {
    let row = RowUuid::from_bytes([0x07; 16]);
    block_on(db.insert(
        "docs",
        BTreeMap::from([
            ("title".to_owned(), Value::String("draft".to_owned())),
            ("rank".to_owned(), Value::I32(4)),
            ("body".to_owned(), Value::String(big_text('b'))),
            ("blob".to_owned(), Value::Bytes(big_bytes(0xab))),
            ("note".to_owned(), some(Value::String(big_text('n')))),
        ]),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .expect("seed row with large values");
    row
}

/// Committed row cells as a public query reader sees them.
fn committed(db: &Db<TestStorage>, row: RowUuid) -> BTreeMap<&'static str, Value> {
    let table = schema().tables()[0].clone();
    let query = db.prepare_query(&db.table("docs")).expect("prepare");
    let rows = db.read(&query).expect("read");
    let row = rows
        .into_iter()
        .find(|candidate| candidate.row_uuid() == row)
        .expect("row is visible");
    ["title", "rank", "body", "blob", "note"]
        .into_iter()
        .map(|column| (column, row.cell(&table, column).expect("column present")))
        .collect()
}

fn rank_patch(rank: i32) -> BTreeMap<String, Value> {
    BTreeMap::from([("rank".to_owned(), Value::I32(rank))])
}

/// A physically plausible descriptor forged by the caller through public
/// Groove types. It was never staged or read by the engine.
fn forged_descriptor() -> Value {
    Value::Large(Box::new(LargeValueRef {
        kind: LargeValueKind::String,
        format_version: FORMAT_VERSION,
        logical_hash: ContentHash([0x11; 32]),
        root: NodeRef {
            object_hash: ContentHash([0x22; 32]),
            locator: Locator::random(),
        },
        byte_length: LARGE_BYTES as u64,
        utf16_length: Some(LARGE_BYTES as u64),
        edit_tail: Vec::new(),
    }))
}

fn assert_large_values_intact(cells: &BTreeMap<&'static str, Value>) {
    assert_eq!(cells["body"], Value::String(big_text('b')));
    assert_eq!(cells["blob"], Value::Bytes(big_bytes(0xab)));
    assert_eq!(cells["note"], some(Value::String(big_text('n'))));
}

/// Contract: inside `db.transaction`, updating one small column of a row
/// whose other columns hold large text, large bytes and a large nullable text
/// commits, the transaction reads its own write, and every large value is
/// unchanged after commit (#3507).
///
/// ```text
/// alice ──insert(200 KB body/blob/note)──► db
/// alice ──tx{ update rank=0; read }──────► db ──commit──► read: rank=0, large intact
/// ```
#[test]
fn transaction_update_of_small_column_keeps_large_values() {
    let db = open_db();
    let row = seed(&db);
    let table = schema().tables()[0].clone();
    let query = db.prepare_query(&db.table("docs")).expect("prepare");

    let ((point, rows), _) = block_on(db.transaction(async |tx| {
        tx.update("docs", row, rank_patch(0), UpdateOptions::default())
            .await?;
        let point = tx.read("docs", row).await?;
        let rows = tx.all_prepared(&query).await?;
        Ok((point, rows))
    }))
    .expect("transactional small-column update commits");

    // Read-your-writes inside the transaction, through both read surfaces.
    let point = point.expect("row visible in its own transaction");
    assert_eq!(point.get("rank"), Some(&Value::I32(0)));
    assert_eq!(point.get("body"), Some(&Value::String(big_text('b'))));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell(&table, "rank"), Some(Value::I32(0)));
    assert_eq!(
        rows[0].cell(&table, "blob"),
        Some(Value::Bytes(big_bytes(0xab)))
    );

    let after = committed(&db, row);
    assert_eq!(after["rank"], Value::I32(0));
    assert_eq!(after["title"], Value::String("draft".to_owned()));
    assert_large_values_intact(&after);
}

/// Contract: two staged patches on the same large-value row within one
/// transaction fold into one commit that keeps the untouched large values.
#[test]
fn transaction_with_repeated_patches_keeps_large_values() {
    let db = open_db();
    let row = seed(&db);

    block_on(db.transaction(async |tx| {
        tx.update("docs", row, rank_patch(1), UpdateOptions::default())
            .await?;
        tx.update(
            "docs",
            row,
            BTreeMap::from([("title".to_owned(), Value::String("final".to_owned()))]),
            UpdateOptions::default(),
        )
        .await
    }))
    .expect("repeated patches commit");

    let after = committed(&db, row);
    assert_eq!(after["rank"], Value::I32(1));
    assert_eq!(after["title"], Value::String("final".to_owned()));
    assert_large_values_intact(&after);
}

/// Contract: a transactional update may replace one large column and null
/// the large nullable column while the other untouched large column is kept.
#[test]
fn transaction_update_of_large_columns_themselves() {
    let db = open_db();
    let row = seed(&db);

    block_on(db.transaction(async |tx| {
        tx.update(
            "docs",
            row,
            BTreeMap::from([
                ("body".to_owned(), Value::String(big_text('c'))),
                ("note".to_owned(), Value::Nullable(None)),
            ]),
            UpdateOptions::default(),
        )
        .await
    }))
    .expect("large-column update commits");

    let after = committed(&db, row);
    assert_eq!(after["body"], Value::String(big_text('c')));
    assert_eq!(after["blob"], Value::Bytes(big_bytes(0xab)));
    assert_eq!(after["note"], Value::Nullable(None));
    assert_eq!(after["rank"], Value::I32(4));
}

/// Contract: a transactional upsert of an existing large-value row behaves
/// like the update above.
#[test]
fn transaction_upsert_of_existing_row_keeps_large_values() {
    let db = open_db();
    let row = seed(&db);

    block_on(db.transaction(async |tx| {
        tx.upsert("docs", row, rank_patch(9), UpsertOptions::default())
            .await
    }))
    .expect("transactional upsert commits");

    let after = committed(&db, row);
    assert_eq!(after["rank"], Value::I32(9));
    assert_large_values_intact(&after);
}

/// Contract: the exclusive transaction path keeps untouched large values
/// when one small column is updated.
#[test]
fn exclusive_transaction_update_keeps_large_values() {
    let db = open_db();
    let row = seed(&db);

    let tx = block_on(db.exclusive_tx()).expect("open exclusive tx");
    block_on(tx.update("docs", row, rank_patch(2), UpdateOptions::default()))
        .expect("stage exclusive update");
    block_on(tx.commit()).expect("exclusive update commits");

    let after = committed(&db, row);
    assert_eq!(after["rank"], Value::I32(2));
    assert_large_values_intact(&after);
}

/// Contract: carrying engine-read descriptors forward does not sanction a
/// caller-authored one. mallory forges a descriptor through public Groove
/// types and patches it into the large column; both the transactional and
/// the standalone update reject it and the stored row is unchanged.
#[test]
fn caller_authored_descriptor_is_still_rejected() {
    let db = open_db();
    let row = seed(&db);
    let forged = || BTreeMap::from([("body".to_owned(), forged_descriptor())]);

    let tx_result = block_on(db.transaction(async |tx| {
        tx.update("docs", row, forged(), UpdateOptions::default())
            .await
    }));
    let tx_error = tx_result.expect_err("transactional update must reject a forged descriptor");
    assert!(
        format!("{tx_error:?}").contains("descriptor"),
        "rejected for the forged descriptor, got {tx_error:?}"
    );

    let standalone = block_on(db.update("docs", row, forged(), UpdateOptions::default()));
    let standalone_error = match standalone {
        Ok(_) => panic!("standalone update must reject a forged descriptor"),
        Err(error) => error,
    };
    assert!(
        format!("{standalone_error:?}").contains("descriptor"),
        "rejected for the forged descriptor, got {standalone_error:?}"
    );

    let after = committed(&db, row);
    assert_eq!(after["rank"], Value::I32(4));
    assert_large_values_intact(&after);
}
