//! U64-stored-as-U48 columns (`INV-STORAGE-39`): rows keep the value in a
//! fixed 6-byte slot, every read and subscription sees a `U64`, and a write
//! of a value above `2^48 - 1` is refused rather than truncated.
//!
//! Most tests assert through the public row API. Where a test reads storage
//! directly, it says why: the 6-byte slot is a durable layout below that API,
//! which a round trip alone cannot tell apart from an 8-byte `U64`.

use super::*;
use crate::records::U48_MAX;
use crate::schema::{ForeignKey, InternedColumnError};

/// `id`, a `stamp` and a nullable `maybe`, both U64-stored-as-U48.
fn stamps_table() -> TableSchema {
    TableSchema::new(
        "stamps",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("stamp", ValueType::u64_stored_as_u48()),
            ColumnSchema::new("maybe", ValueType::u64_stored_as_u48().nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
}

fn stamp_row(id: u64, stamp: u64, maybe: Option<u64>) -> Vec<Value> {
    vec![
        Value::U64(id),
        Value::U64(stamp),
        Value::Nullable(maybe.map(|value| Box::new(Value::U64(value)))),
    ]
}

async fn stamps_database() -> (Database, MemoryStorage) {
    let schema = DatabaseSchema::new([stamps_table()]);
    let storage = MemoryStorage::new(&schema.column_families()).unwrap();
    let database = Database::new(schema, storage.clone()).await.unwrap();
    (database, storage)
}

/// The stored payload of row `id`.
async fn stored_payload(storage: &MemoryStorage, id: u64) -> Vec<u8> {
    let mut key = Vec::new();
    super::super::encoding::encode_primary_key_part(&mut key, &Value::U64(id)).unwrap();
    let stored = storage
        .get("stamps".to_owned(), key)
        .await
        .unwrap()
        .expect("stored row");
    version_zero_payload(&stored).to_vec()
}

/// Contract: point and scan reads of a U64-stored-as-U48 column return the
/// `U64` written, including `2^48 - 1` and a nullable null, while the stored
/// row keeps each value in exactly 6 little-endian bytes.
///
/// Reads storage directly: the 6-byte slot is below the row API.
#[futures_test::test]
async fn stored_u48_columns_round_trip_as_u64_in_six_byte_slots() {
    let (mut database, storage) = stamps_database().await;
    let mut batch = database.open_batch();
    batch.insert("stamps", stamp_row(1, 0x0102_0304_0506, Some(U48_MAX)));
    batch.insert("stamps", stamp_row(2, 0, None));
    database.commit_batch(batch).await.unwrap();

    let logical = database.table_schema("stamps").unwrap().record_schema();
    assert_eq!(
        logical.fields()[1].value_type,
        ValueType::U64,
        "reads see the logical U64"
    );
    let one = database
        .primary_key_get("stamps", &[Value::U64(1)])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        one.record().to_values().unwrap(),
        stamp_row(1, 0x0102_0304_0506, Some(U48_MAX))
    );
    let scanned = database
        .primary_key_scan("stamps", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.record().to_values().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        scanned,
        vec![
            stamp_row(1, 0x0102_0304_0506, Some(U48_MAX)),
            stamp_row(2, 0, None)
        ]
    );

    // Byte pin: id U64 (8 bytes), stamp 6 bytes, maybe flag + 6 bytes.
    assert_eq!(
        stored_payload(&storage, 1).await,
        [
            &[1, 0, 0, 0, 0, 0, 0, 0][..],
            &[0x06, 0x05, 0x04, 0x03, 0x02, 0x01],
            &[1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ]
        .concat()
    );
    assert_eq!(
        stored_payload(&storage, 2).await,
        [&[2, 0, 0, 0, 0, 0, 0, 0][..], &[0; 6], &[0; 7]].concat()
    );
}

/// Contract: a write whose U64-stored-as-U48 value exceeds `2^48 - 1` fails
/// with `U48OutOfRange` and stores nothing; the value is never truncated.
#[futures_test::test]
async fn stored_u48_columns_refuse_values_above_the_slot() {
    let (mut database, storage) = stamps_database().await;
    for row in [
        stamp_row(1, U48_MAX + 1, None),
        stamp_row(1, 0, Some(u64::MAX)),
    ] {
        let mut batch = database.open_batch();
        batch.insert("stamps", row);
        assert!(matches!(
            database.commit_batch(batch).await,
            Err(Error::RecordEncoding(crate::records::Error::U48OutOfRange(
                _
            )))
        ));
    }
    assert!(
        database
            .primary_key_get("stamps", &[Value::U64(1)])
            .await
            .unwrap()
            .is_none()
    );
    let mut key = Vec::new();
    super::super::encoding::encode_primary_key_part(&mut key, &Value::U64(1)).unwrap();
    assert!(
        storage
            .get("stamps".to_owned(), key)
            .await
            .unwrap()
            .is_none()
    );
}

/// Contract: subscriptions over a U64-stored-as-U48 column see `U64` values
/// and filter them against `U64` literals.
#[futures_test::test]
async fn subscriptions_over_stored_u48_columns_see_u64() {
    let (mut database, _) = stamps_database().await;
    let mut batch = database.open_batch();
    batch.insert("stamps", stamp_row(1, 10, None));
    batch.insert("stamps", stamp_row(2, 20, Some(5)));
    database.commit_batch(batch).await.unwrap();

    let late = database
        .subscribe_one_sink(
            GraphBuilder::table("stamps").filter(PredicateExpr::gt("stamp", Value::U64(15))),
        )
        .await
        .unwrap();
    assert_eq!(
        late.recv().unwrap().to_values().unwrap(),
        vec![(stamp_row(2, 20, Some(5)), 1)]
    );
    let mut batch = database.open_batch();
    batch.insert("stamps", stamp_row(3, U48_MAX, None));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        late.recv().unwrap().to_values().unwrap(),
        vec![(stamp_row(3, U48_MAX, None), 1)]
    );
}

/// Contract: schemas that place a U64-stored-as-U48 column where Groove
/// cannot transcode it (nested, key, index, foreign key, direct record store)
/// are refused with a typed error.
#[futures_test::test]
async fn unsupported_stored_u48_shapes_are_refused() {
    let stamp = ValueType::u64_stored_as_u48();
    let base = |column_type: ValueType| {
        TableSchema::new(
            "stamps",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("stamp", column_type),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    };
    let refused = |schema: DatabaseSchema| async move {
        let storage = MemoryStorage::new(&schema.column_families()).unwrap();
        match Database::new(schema, storage).await {
            Err(Error::InvalidInternedColumn(InternedColumnError::StoredU48Placement {
                placement,
                ..
            })) => placement,
            other => panic!("expected a stored-U48 refusal, got {:?}", other.err()),
        }
    };

    let keyed = TableSchema::new("keyed", [ColumnSchema::new("stamp", stamp.clone())])
        .with_primary_key(PrimaryKey::new("stamp", IntegerKeyType::U64));
    assert_eq!(
        refused(DatabaseSchema::new([keyed])).await,
        "as a primary-key column"
    );
    assert_eq!(
        refused(DatabaseSchema::new([
            base(stamp.clone()).with_index(IndexSchema::new("by_stamp", ["stamp"]))
        ]))
        .await,
        "as an index column"
    );
    assert_eq!(
        refused(DatabaseSchema::new([base(stamp.clone()).with_foreign_key(
            ForeignKey::new("stamp_fk", ["stamp"], "stamps", ["id"])
        )]))
        .await,
        "as a foreign-key column"
    );
    for nested in [
        stamp.clone().array_of(),
        stamp.clone().nullable().nullable(),
        ValueType::Tuple(vec![stamp.clone()]),
        ValueType::Record(Box::new(RecordDescriptor::new([("a", stamp.clone())]))),
    ] {
        assert_eq!(
            refused(DatabaseSchema::new([base(nested)])).await,
            "anywhere but at top level or directly under a top-level nullable"
        );
    }
    assert_eq!(
        refused(
            DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
                "store",
                RecordDescriptor::new([("id", ValueType::U64)]),
                RecordDescriptor::new([("stamp", stamp.clone())]),
            ))
        )
        .await,
        "in a direct record store"
    );
    let ok = DatabaseSchema::new([base(stamp)]);
    let storage = MemoryStorage::new(&ok.column_families()).unwrap();
    Database::new(ok, storage).await.unwrap();
}

/// Contract: a descriptor carrying a U64-stored-as-U48 column never crosses a
/// persisted or wire descriptor encoding; its logical descriptor does.
#[test]
fn descriptors_with_stored_u48_columns_are_not_encodable() {
    let table = stamps_table();
    let physical = table.storage_record_schema();
    assert_eq!(
        crate::records::encode_record_descriptor(&physical),
        Err(crate::records::Error::StoredU48TypeNotEncodable)
    );
    assert_eq!(
        crate::records::encode_persisted_record_descriptor(&physical),
        Err(crate::records::Error::StoredU48TypeNotEncodable)
    );
    let logical = table.record_schema();
    assert!(!logical.contains_storage_transcoding());
    let encoded = crate::records::encode_persisted_record_descriptor(&logical).unwrap();
    assert_eq!(
        crate::records::decode_persisted_record_descriptor(&encoded).unwrap(),
        logical
    );
}
