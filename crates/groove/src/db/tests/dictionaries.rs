//! Interned (dictionary-encoded) columns: rows store a 4-byte code, every
//! read and subscription sees the value.
//!
//! Most tests here assert through the public row API. Where a test also reads
//! storage directly, it says why: the code-in-place layout and the dictionary
//! entries are durable bytes below that API, which a round trip alone cannot
//! distinguish from storing the full value.

use super::*;
use crate::dictionary::{DictionaryError, dictionary_column_family};
use crate::records::OwnedRecord;
use crate::schema::DictionarySchema;
use crate::storage::ScanRequest;

const AUTHORS: &str = "authors";

fn author_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("name", ValueType::String), ("org", ValueType::Uuid)])
}

fn author_type() -> ValueType {
    ValueType::Record(Box::new(author_descriptor()))
}

fn author(name: &str) -> Value {
    let descriptor = author_descriptor();
    let raw = descriptor
        .create(&[Value::String(name.into()), Value::Uuid(uuid(7))])
        .unwrap();
    Value::Record(OwnedRecord::new(raw, descriptor))
}

fn author_bytes(name: &str) -> Vec<u8> {
    let Value::Record(record) = author(name) else {
        unreachable!()
    };
    record.raw().to_vec()
}

/// `id`, an interned `author`, a nullable interned `editor` and an indexed
/// plain `score`.
fn posts_table(name: &str) -> TableSchema {
    TableSchema::new(
        name,
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("author", ValueType::interned(AUTHORS, author_type())),
            ColumnSchema::new(
                "editor",
                ValueType::interned(AUTHORS, author_type()).nullable(),
            ),
            ColumnSchema::new("score", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new(format!("{name}_by_score"), ["score"]))
}

fn authors_dictionary() -> DictionarySchema {
    DictionarySchema::new(AUTHORS, author_type())
}

fn posts_schema() -> DatabaseSchema {
    DatabaseSchema::new([posts_table("posts"), posts_table("drafts")])
        .with_dictionary(authors_dictionary())
}

fn posts_storage(schema: &DatabaseSchema) -> MemoryStorage {
    MemoryStorage::new(&schema.column_families()).unwrap()
}

fn post(id: u64, author_name: &str, editor: Option<&str>, score: u64) -> Vec<Value> {
    vec![
        Value::U64(id),
        author(author_name),
        Value::Nullable(editor.map(|name| Box::new(author(name)))),
        Value::U64(score),
    ]
}

async fn insert_posts(database: &mut Database, table: &str, rows: Vec<Vec<Value>>) {
    let mut batch = database.open_batch();
    for row in rows {
        batch.insert(table, row);
    }
    database.commit_batch(batch).await.unwrap();
}

async fn dictionary_entries<S: OrderedKvStorage>(storage: &S) -> Vec<(Vec<u8>, Vec<u8>)> {
    crate::storage::collect_scan(
        storage
            .scan(ScanRequest::prefix(
                dictionary_column_family(AUTHORS),
                Vec::new(),
            ))
            .await
            .unwrap(),
    )
    .await
    .unwrap()
}

/// The storage key of row `id` of a `u64`-keyed table.
fn row_key(id: u64) -> Vec<u8> {
    let mut key = Vec::new();
    super::super::encoding::encode_primary_key_part(&mut key, &Value::U64(id)).unwrap();
    key
}

/// The stored payload of one row, bound with the table's physical descriptor.
async fn stored_row(storage: &MemoryStorage, table: &str, id: u64) -> Vec<Value> {
    let stored = storage
        .get(table.to_owned(), row_key(id))
        .await
        .unwrap()
        .expect("stored row");
    let descriptor = posts_table(table).storage_record_schema();
    descriptor
        .bind(version_zero_payload(&stored))
        .to_values()
        .unwrap()
}

/// Contract: point, scan and index reads of a table with interned columns
/// return full logical values, while each stored row holds 4-byte codes and
/// the dictionary holds one entry per distinct value.
///
/// Actors: `alice` and `bob` author three posts; `alice` edits one, one has
/// no editor (nullable interned column, null and present).
///
/// Reads storage directly: the code-in-place layout is below the row API.
#[futures_test::test]
async fn interned_columns_round_trip_through_point_scan_and_index_reads() {
    let schema = posts_schema();
    let storage = posts_storage(&schema);
    let mut database = Database::new(schema, storage.clone()).await.unwrap();
    insert_posts(
        &mut database,
        "posts",
        vec![
            post(1, "alice", None, 10),
            post(2, "bob", Some("alice"), 20),
            post(3, "alice", Some("bob"), 20),
        ],
    )
    .await;

    let one = database
        .primary_key_get("posts", &[Value::U64(1)])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(one.get("author").unwrap(), author("alice"));
    assert_eq!(one.get("editor").unwrap(), Value::Nullable(None));
    assert_eq!(
        one.record().descriptor(),
        &database.table_schema("posts").unwrap().record_schema()
    );

    let scanned = database
        .primary_key_scan("posts", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.record().to_values().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        scanned,
        vec![
            post(1, "alice", None, 10),
            post(2, "bob", Some("alice"), 20),
            post(3, "alice", Some("bob"), 20),
        ]
    );

    let indexed = database
        .index_scan("posts", "posts_by_score", &[Value::U64(20)])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.record().to_values().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        indexed,
        vec![
            post(2, "bob", Some("alice"), 20),
            post(3, "alice", Some("bob"), 20),
        ]
    );

    // alice was interned first, so she is code 0; bob is code 1.
    assert_eq!(
        stored_row(&storage, "posts", 2).await,
        vec![
            Value::U64(2),
            Value::U32(1),
            Value::Nullable(Some(Box::new(Value::U32(0)))),
            Value::U64(20),
        ]
    );
    assert_eq!(
        stored_row(&storage, "posts", 1).await[2],
        Value::Nullable(None)
    );
    assert_eq!(
        dictionary_entries(&storage).await,
        vec![
            (0u32.to_be_bytes().to_vec(), author_bytes("alice")),
            (1u32.to_be_bytes().to_vec(), author_bytes("bob")),
        ]
    );
}

/// Contract: a dictionary is shared by every interned column that names it,
/// across tables: the same value has the same code everywhere and one entry.
///
/// Actors: `bob` authors a post and a draft; `carol` edits the draft.
///
/// Reads storage directly: codes are below the row API.
#[futures_test::test]
async fn tables_sharing_a_dictionary_store_the_same_code() {
    let schema = posts_schema();
    let storage = posts_storage(&schema);
    let mut database = Database::new(schema, storage.clone()).await.unwrap();
    insert_posts(&mut database, "posts", vec![post(1, "bob", None, 1)]).await;
    insert_posts(
        &mut database,
        "drafts",
        vec![post(9, "carol", Some("bob"), 1)],
    )
    .await;

    assert_eq!(stored_row(&storage, "posts", 1).await[1], Value::U32(0));
    let draft = stored_row(&storage, "drafts", 9).await;
    assert_eq!(draft[1], Value::U32(1));
    assert_eq!(draft[2], Value::Nullable(Some(Box::new(Value::U32(0)))));
    assert_eq!(dictionary_entries(&storage).await.len(), 2);
    assert_eq!(
        database
            .primary_key_get("drafts", &[Value::U64(9)])
            .await
            .unwrap()
            .unwrap()
            .record()
            .to_values()
            .unwrap(),
        post(9, "carol", Some("bob"), 1)
    );
}

/// Contract: a code allocated for a batch that is dropped stays reserved for
/// its value. No other value receives it, and its entry is written by the
/// next publication, so a later row reusing the value names a durable entry.
///
/// ```text
/// batch₁: carol ─prepare─► code 2 ─drop
/// batch₂: dave  ─prepare─► code 3 ─commit─► entries 2 (carol) and 3 (dave)
/// batch₃: carol ─prepare─► code 2 ─commit─► no new entry
/// ```
///
/// Reads storage directly: entries and codes are below the row API.
#[futures_test::test]
async fn a_dropped_batch_keeps_its_code_reserved_and_writes_the_entry_later() {
    let schema = posts_schema();
    let storage = posts_storage(&schema);
    let mut database = Database::new(schema, storage.clone()).await.unwrap();
    insert_posts(
        &mut database,
        "posts",
        vec![post(1, "alice", Some("bob"), 1)],
    )
    .await;

    let mut dropped = database.open_batch();
    dropped.insert("posts", post(2, "carol", None, 1));
    // Reading through the batch prepares its writes, which allocates.
    let staged = database
        .primary_key_get_raw_in_batch(&dropped, "posts", &[Value::U64(2)])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(staged.record().get("author").unwrap(), author("carol"));
    drop(dropped);
    assert_eq!(dictionary_entries(&storage).await.len(), 2);

    insert_posts(&mut database, "posts", vec![post(3, "dave", None, 1)]).await;
    assert_eq!(stored_row(&storage, "posts", 3).await[1], Value::U32(3));
    let entries = dictionary_entries(&storage).await;
    assert_eq!(entries.len(), 4);
    assert_eq!(
        entries[2],
        (2u32.to_be_bytes().to_vec(), author_bytes("carol"))
    );

    insert_posts(&mut database, "posts", vec![post(4, "carol", None, 1)]).await;
    assert_eq!(stored_row(&storage, "posts", 4).await[1], Value::U32(2));
    assert_eq!(dictionary_entries(&storage).await.len(), 4);
}

/// Contract: a publication that interns a new value is still exactly one
/// storage write: the dictionary entry rides the row's atomic `write_many`.
///
/// Actors: `alice` authors one post.
#[futures_test::test]
async fn a_new_entry_rides_the_publication_s_single_storage_write() {
    let schema = posts_schema();
    let (storage, control) = crate::storage::TestStorage::controlled(&schema.column_families());
    let mut database = Database::new(schema, storage.clone()).await.unwrap();
    control.take_observed();

    let mut batch = database.open_batch();
    batch.insert("posts", post(1, "alice", None, 1));
    let applied = database.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();

    let writes = control
        .take_observed()
        .into_iter()
        .filter(|operation| {
            matches!(
                operation,
                TestStorageOperation::WriteMany
                    | TestStorageOperation::Set
                    | TestStorageOperation::Delete
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(writes, vec![TestStorageOperation::WriteMany]);
    assert_eq!(dictionary_entries(&storage).await.len(), 1);
}

/// Contract: a reopened database loads its dictionaries, decodes stored rows
/// to the same values, and continues allocating after the highest code.
///
/// Actors: `alice` and `bob` write before the restart, `carol` after.
#[futures_test::test]
async fn a_reopened_database_decodes_rows_and_continues_allocation() {
    let schema = posts_schema();
    let storage = posts_storage(&schema);
    let mut database = Database::new(schema.clone(), storage.clone())
        .await
        .unwrap();
    insert_posts(
        &mut database,
        "posts",
        vec![post(1, "alice", Some("bob"), 1)],
    )
    .await;
    drop(database);

    let mut reopened = Database::new(schema, storage.clone()).await.unwrap();
    assert_eq!(
        reopened
            .primary_key_get("posts", &[Value::U64(1)])
            .await
            .unwrap()
            .unwrap()
            .record()
            .to_values()
            .unwrap(),
        post(1, "alice", Some("bob"), 1)
    );
    insert_posts(
        &mut reopened,
        "posts",
        vec![post(2, "carol", Some("alice"), 1)],
    )
    .await;
    let stored = stored_row(&storage, "posts", 2).await;
    assert_eq!(stored[1], Value::U32(2));
    assert_eq!(stored[2], Value::Nullable(Some(Box::new(Value::U32(0)))));
}

/// Contract: opening refuses a corrupt dictionary with a typed error instead
/// of decoding rows against it: a repeated value, a non-canonical value, a
/// malformed key and a gap in the codes. A row naming a code with no entry is
/// refused when read.
///
/// Writes storage directly: corruption cannot be produced through the API.
#[futures_test::test]
async fn a_corrupt_dictionary_is_refused() {
    let schema = posts_schema();
    let family = dictionary_column_family(AUTHORS);
    let corruptions: Vec<Vec<(Vec<u8>, Vec<u8>)>> = vec![
        vec![
            (0u32.to_be_bytes().to_vec(), author_bytes("alice")),
            (1u32.to_be_bytes().to_vec(), author_bytes("alice")),
        ],
        vec![(0u32.to_be_bytes().to_vec(), vec![0xff, 0x00])],
        vec![(vec![0, 0, 0], author_bytes("alice"))],
        vec![(1u32.to_be_bytes().to_vec(), author_bytes("alice"))],
    ];
    for entries in corruptions {
        let storage = posts_storage(&schema);
        for (key, value) in entries {
            storage.set(family.clone(), key, value).await.unwrap();
        }
        let error = Database::new(schema.clone(), storage)
            .await
            .err()
            .expect("a corrupt dictionary is refused");
        assert!(
            matches!(
                error,
                Error::Dictionary(DictionaryError::InvalidDictionary { .. })
            ),
            "{error:?}"
        );
    }

    let storage = posts_storage(&schema);
    let physical = posts_table("posts").storage_record_schema();
    let payload = physical
        .create(&[
            Value::U64(1),
            Value::U32(5),
            Value::Nullable(None),
            Value::U64(1),
        ])
        .unwrap();
    storage
        .set(
            "posts".to_owned(),
            row_key(1),
            crate::records::encode_variant_record(0, &payload),
        )
        .await
        .unwrap();
    let database = Database::new(schema, storage).await.unwrap();
    assert!(matches!(
        database.primary_key_get("posts", &[Value::U64(1)]).await,
        Err(Error::Dictionary(DictionaryError::UnknownCode {
            code: 5,
            ..
        }))
    ));
}

/// Contract: subscriptions over a table with interned columns see logical
/// values: a projection of a field nested in the interned record, a filter on
/// it, and the deltas of inserts, updates and deletes (whose retracted old
/// rows were read back from stored codes).
///
/// ```text
/// alice ─insert 1,2─► hydrate {1, 2}
/// alice ─insert 3───► +3
/// bob   ─update 2───► -2 (author alice → bob)
/// alice ─delete 1───► -1
/// ```
#[futures_test::test]
async fn subscriptions_over_interned_columns_see_logical_deltas() {
    let schema = posts_schema();
    let storage = posts_storage(&schema);
    let mut database = Database::new(schema, storage).await.unwrap();
    insert_posts(
        &mut database,
        "posts",
        vec![
            post(1, "alice", None, 1),
            post(2, "alice", Some("bob"), 2),
            post(4, "bob", None, 4),
        ],
    )
    .await;

    let by_alice = database
        .subscribe_one_sink(
            GraphBuilder::table("posts")
                .project_fields([
                    ProjectField::named("id"),
                    ProjectField::record_field("author", ["name"], "author_name"),
                ])
                .filter(PredicateExpr::eq(
                    "author_name",
                    Value::String("alice".into()),
                )),
        )
        .await
        .unwrap();
    let whole = database
        .subscribe_one_sink(GraphBuilder::table("posts"))
        .await
        .unwrap();
    let alice = |id| vec![Value::U64(id), Value::String("alice".into())];
    let mut initial = by_alice.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[0] {
        Value::U64(id) => id,
        _ => unreachable!(),
    });
    assert_eq!(initial, vec![(alice(1), 1), (alice(2), 1)]);
    assert_eq!(whole.recv().unwrap().to_values().unwrap().len(), 3);

    insert_posts(&mut database, "posts", vec![post(3, "alice", None, 3)]).await;
    assert_eq!(
        by_alice.recv().unwrap().to_values().unwrap(),
        vec![(alice(3), 1)]
    );
    assert_eq!(
        whole.recv().unwrap().to_values().unwrap(),
        vec![(post(3, "alice", None, 3), 1)]
    );

    let mut update = database.open_batch();
    update.update("posts", post(2, "bob", Some("bob"), 2));
    database.commit_batch(update).await.unwrap();
    assert_eq!(
        by_alice.recv().unwrap().to_values().unwrap(),
        vec![(alice(2), -1)]
    );
    let mut updated = whole.recv().unwrap().to_values().unwrap();
    updated.sort_by_key(|(_, weight)| *weight);
    assert_eq!(
        updated,
        vec![
            (post(2, "alice", Some("bob"), 2), -1),
            (post(2, "bob", Some("bob"), 2), 1),
        ]
    );

    let mut delete = database.open_batch();
    delete.delete("posts", PrimaryKeyValue::U64(1));
    database.commit_batch(delete).await.unwrap();
    assert_eq!(
        by_alice.recv().unwrap().to_values().unwrap(),
        vec![(alice(1), -1)]
    );
}

/// Contract: an index registered on a live table with interned columns is
/// backfilled from the stored rows, which the backfill reads as logical
/// records.
///
/// Actors: `alice` and `bob` post before the index exists.
#[futures_test::test]
async fn an_index_registered_later_backfills_from_interned_rows() {
    let schema = posts_schema();
    let storage = posts_storage(&schema);
    let mut database = Database::new(schema, storage).await.unwrap();
    insert_posts(
        &mut database,
        "drafts",
        vec![post(1, "alice", None, 5), post(2, "bob", Some("alice"), 6)],
    )
    .await;
    database
        .register_table_index("drafts", IndexSchema::new("drafts_by_id", ["id"]))
        .await
        .unwrap();
    let rows = database
        .index_scan("drafts", "drafts_by_id", &[Value::U64(2)])
        .await
        .unwrap();
    assert_eq!(
        rows[0].record().to_values().unwrap(),
        post(2, "bob", Some("alice"), 6)
    );
}

/// Contract: `ensure_exact` compares the stored form of a logical record, so
/// re-ensuring an identical row with an interned column is idempotent.
///
/// Actors: `alice` writes the same immutable row twice.
#[futures_test::test]
async fn ensure_exact_is_idempotent_for_interned_rows() {
    let schema = posts_schema();
    let storage = posts_storage(&schema);
    let mut database = Database::new(schema, storage).await.unwrap();
    let logical = database.table_schema("posts").unwrap().record_schema();
    let payload = logical.create(&post(1, "alice", Some("alice"), 1)).unwrap();

    let mut first = database.open_batch();
    assert_eq!(
        first
            .ensure_exact(&database, "posts", PrimaryKeyValue::U64(1), payload.clone())
            .await
            .unwrap(),
        EnsureExactOutcome::Inserted
    );
    database.commit_batch(first).await.unwrap();

    let mut second = database.open_batch();
    assert_eq!(
        second
            .ensure_exact(&database, "posts", PrimaryKeyValue::U64(1), payload)
            .await
            .unwrap(),
        EnsureExactOutcome::AlreadyIdentical
    );
    let different = logical.create(&post(1, "bob", Some("alice"), 1)).unwrap();
    let mut third = database.open_batch();
    assert_eq!(
        third
            .ensure_exact(&database, "posts", PrimaryKeyValue::U64(1), different)
            .await
            .unwrap(),
        EnsureExactOutcome::Conflict
    );
}

/// Contract: interning is a property of the physical column, so it holds in
/// every variant payload that declares an interned field, and each variant
/// reads back logical values.
///
/// Actors: `alice` writes a row in each of two layouts.
#[futures_test::test]
async fn variant_payload_fields_keep_their_interning() {
    let interned = ValueType::interned(AUTHORS, author_type());
    let table = TableSchema::new(
        "events",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("by", interned.clone()),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_variant_payload(
        1,
        [
            TableVariantField::shared("id", ColumnType::U64, "id"),
            TableVariantField::shared("by", interned.clone(), "by"),
        ],
    )
    .with_variant_payload(
        2,
        [
            TableVariantField::shared("id", ColumnType::U64, "id"),
            TableVariantField::local("note", ColumnType::String),
            TableVariantField::shared("by", interned, "by"),
        ],
    );
    let schema = DatabaseSchema::new([table.clone()]).with_dictionary(authors_dictionary());
    let storage = MemoryStorage::new(&schema.column_families()).unwrap();
    let mut database = Database::new(schema, storage.clone()).await.unwrap();
    let first = table.record_schema_for_variant(1).unwrap();
    let second = table.record_schema_for_variant(2).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "events",
        VariantRecord::new(
            1,
            OwnedRecord::new(
                first.create(&[Value::U64(1), author("alice")]).unwrap(),
                first,
            ),
        ),
    );
    batch.insert(
        "events",
        VariantRecord::new(
            2,
            OwnedRecord::new(
                second
                    .create(&[Value::U64(2), Value::String("hi".into()), author("alice")])
                    .unwrap(),
                second,
            ),
        ),
    );
    database.commit_batch(batch).await.unwrap();

    let rows = database.primary_key_scan("events", &[]).await.unwrap();
    assert_eq!(rows[0].record().get("by").unwrap(), author("alice"));
    assert_eq!(rows[1].record().get("by").unwrap(), author("alice"));
    let stored = storage
        .get("events".to_owned(), row_key(2))
        .await
        .unwrap()
        .unwrap();
    let (tag, payload) = crate::records::split_variant_record(&stored).unwrap();
    assert_eq!(tag, 2);
    let physical = table.storage_record_schema_for_variant(2).unwrap();
    assert_eq!(physical.bind(payload).get("by").unwrap(), Value::U32(0));
    assert_eq!(dictionary_entries(&storage).await.len(), 1);

    // The database serves the same logical descriptors from its codec cache:
    // repeated lookups build nothing, and an unknown variant is `None`.
    assert_eq!(
        database.table_record_descriptor("events", 1).unwrap(),
        Some(first)
    );
    assert_eq!(
        database.table_record_descriptor("events", 2).unwrap(),
        Some(second)
    );
    crate::schema::VARIANT_DESCRIPTOR_BUILDS.with(|count| count.set(0));
    for _ in 0..3 {
        assert_eq!(
            database.table_record_descriptor("events", 2).unwrap(),
            Some(second)
        );
    }
    assert_eq!(
        crate::schema::VARIANT_DESCRIPTOR_BUILDS.with(|count| count.get()),
        0
    );
    assert_eq!(database.table_record_descriptor("events", 9).unwrap(), None);
}

/// Contract: schemas that place an interned column where Groove cannot
/// maintain it are refused with a typed error naming the pattern.
#[futures_test::test]
async fn unsupported_interned_shapes_are_refused() {
    use crate::schema::{ForeignKey, InternedColumnError};
    let interned = ValueType::interned(AUTHORS, author_type());
    let base = |column_type: ValueType| {
        TableSchema::new(
            "posts",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("author", column_type),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    };
    let refused = |schema: DatabaseSchema| async move {
        let storage = MemoryStorage::new(&schema.column_families()).unwrap();
        match Database::new(schema, storage).await {
            Err(Error::InvalidInternedColumn(error)) => error,
            other => panic!("expected an interned column refusal, got {:?}", other.err()),
        }
    };
    let with_authors =
        |table: TableSchema| DatabaseSchema::new([table]).with_dictionary(authors_dictionary());

    let key_table = TableSchema::new("keyed", [ColumnSchema::new("author", interned.clone())])
        .with_primary_key(PrimaryKey::new("author", IntegerKeyType::U64));
    assert!(matches!(
        refused(with_authors(key_table)).await,
        InternedColumnError::PrimaryKey { .. }
    ));
    assert!(matches!(
        refused(with_authors(
            base(interned.clone()).with_index(IndexSchema::new("by_author", ["author"]))
        ))
        .await,
        InternedColumnError::Index { .. }
    ));
    assert!(matches!(
        refused(with_authors(base(interned.clone()).with_foreign_key(
            ForeignKey::new("author_fk", ["author"], "posts", ["id"])
        )))
        .await,
        InternedColumnError::ForeignKey { .. }
    ));
    for nested in [
        interned.clone().array_of(),
        interned.clone().nullable().nullable(),
        ValueType::Record(Box::new(RecordDescriptor::new([("a", interned.clone())]))),
        ValueType::interned(AUTHORS, interned.clone()),
    ] {
        assert!(matches!(
            refused(with_authors(base(nested))).await,
            InternedColumnError::Nested { .. }
        ));
    }
    assert!(matches!(
        refused(DatabaseSchema::new([base(interned.clone())])).await,
        InternedColumnError::UnknownDictionary { .. }
    ));
    assert!(matches!(
        refused(with_authors(base(ValueType::interned(
            AUTHORS,
            ValueType::String
        ))))
        .await,
        InternedColumnError::ValueTypeMismatch { .. }
    ));
    let enum_type = ValueType::EnumTag(ScalarEnumSchema::new("kind", ["a", "b"]).unwrap());
    assert!(matches!(
        refused(
            DatabaseSchema::new([base(ValueType::interned("kinds", enum_type.clone()))])
                .with_dictionary(DictionarySchema::new("kinds", enum_type))
        )
        .await,
        InternedColumnError::UnsupportedValueType { .. }
    ));
    let stored_scalar = crate::large_values::physical_storage_value_type(
        crate::large_values::LargeValueKind::Bytes,
    );
    assert!(matches!(
        refused(
            DatabaseSchema::new([base(ValueType::interned("blobs", stored_scalar.clone()))])
                .with_dictionary(DictionarySchema::new("blobs", stored_scalar))
        )
        .await,
        InternedColumnError::UnsupportedValueType { .. }
    ));
    assert!(matches!(
        refused(
            DatabaseSchema::new([])
                .with_direct_record_store(DirectRecordStoreSchema::new(
                    "store",
                    RecordDescriptor::new([("id", ValueType::U64)]),
                    RecordDescriptor::new([("author", interned.clone())]),
                ))
                .with_dictionary(authors_dictionary())
        )
        .await,
        InternedColumnError::DirectRecordStore { .. }
    ));

    // Live registration is held to the same rules.
    let schema = DatabaseSchema::new([]).with_dictionary(authors_dictionary());
    let storage = MemoryStorage::new(&schema.column_families()).unwrap();
    let mut database = Database::new(schema, storage).await.unwrap();
    assert!(matches!(
        database.register_table(
            base(interned.clone()).with_index(IndexSchema::new("by_author", ["author"]))
        ),
        Err(Error::InvalidInternedColumn(
            InternedColumnError::Index { .. }
        ))
    ));
    database.register_table(base(interned)).unwrap();
}

/// Contract: a descriptor carrying an interned column never crosses a
/// persisted or wire descriptor encoding; its logical descriptor does.
#[test]
fn descriptors_with_interned_columns_are_not_encodable() {
    let table = posts_table("posts");
    let physical = table.storage_record_schema();
    let expected = crate::records::Error::InternedTypeNotEncodable {
        dictionary: AUTHORS.to_owned(),
    };
    assert_eq!(
        crate::records::encode_record_descriptor(&physical),
        Err(expected.clone())
    );
    assert_eq!(
        crate::records::encode_persisted_record_descriptor(&physical),
        Err(expected)
    );
    let logical = table.record_schema();
    assert!(!logical.contains_interned());
    let encoded = crate::records::encode_persisted_record_descriptor(&logical).unwrap();
    assert_eq!(
        crate::records::decode_persisted_record_descriptor(&encoded).unwrap(),
        logical
    );
}

/// Contract: a rebuild keeps the dictionaries the database opened with, but
/// cannot declare one the open did not load.
#[futures_test::test]
async fn a_rebuild_cannot_declare_an_unloaded_dictionary() {
    let schema = DatabaseSchema::new([]).with_dictionary(authors_dictionary());
    let storage = MemoryStorage::new(&posts_schema().column_families()).unwrap();
    let mut database = Database::new(schema, storage).await.unwrap();
    database.prepare_for_storage_extraction().await.unwrap();
    let extra = posts_schema().with_dictionary(DictionarySchema::new("tags", ValueType::String));
    assert!(matches!(
        database.rebuild(extra),
        Err(Error::Dictionary(DictionaryError::NotLoaded(name))) if name == "tags"
    ));
}

/// Contract: a rebuilt database shares the dictionaries of the one it
/// replaces, so codes allocated before the rebuild keep their meaning.
#[futures_test::test]
async fn a_rebuild_shares_loaded_dictionaries() {
    let storage = MemoryStorage::new(&posts_schema().column_families()).unwrap();
    let mut database = Database::new(posts_schema(), storage).await.unwrap();
    insert_posts(&mut database, "posts", vec![post(1, "alice", None, 1)]).await;
    database.prepare_for_storage_extraction().await.unwrap();
    database.rebuild(posts_schema()).unwrap();
    insert_posts(
        &mut database,
        "posts",
        vec![post(2, "bob", Some("alice"), 1)],
    )
    .await;
    let rows = database.primary_key_scan("posts", &[]).await.unwrap();
    assert_eq!(
        rows[1].record().to_values().unwrap(),
        post(2, "bob", Some("alice"), 1)
    );
}

/// Contract (`groove.dictionary.v1` bytes): a dictionary entry is keyed by
/// its 4-byte big-endian code and holds the canonical value encoding; a row
/// stores the code as a 4-byte little-endian field, and a nullable interned
/// field as a presence byte and the code (zero-filled when null).
///
/// Reads storage directly: these are the durable bytes this family freezes.
#[futures_test::test]
async fn dictionary_v1_entry_and_row_bytes_are_pinned() {
    let table = TableSchema::new(
        "tags",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("tag", ValueType::interned("names", ValueType::String)),
            ColumnSchema::new(
                "alias",
                ValueType::interned("names", ValueType::String).nullable(),
            ),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64));
    let schema = DatabaseSchema::new([table])
        .with_dictionary(DictionarySchema::new("names", ValueType::String));
    assert!(
        schema
            .column_families()
            .contains(&"__groove_dictionary_names")
    );
    let storage = MemoryStorage::new(&schema.column_families()).unwrap();
    let mut database = Database::new(schema, storage.clone()).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "tags",
        vec![
            Value::U64(1),
            Value::String("red".into()),
            Value::Nullable(None),
        ],
    );
    batch.insert(
        "tags",
        vec![
            Value::U64(2),
            Value::String("blue".into()),
            Value::Nullable(Some(Box::new(Value::String("red".into())))),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let entries = crate::storage::collect_scan(
        storage
            .scan(ScanRequest::prefix(
                "__groove_dictionary_names".to_owned(),
                Vec::new(),
            ))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        entries,
        vec![
            // code 0 → "red": inline string scalar (tag 00, then UTF-8).
            (vec![0, 0, 0, 0], vec![0x00, b'r', b'e', b'd']),
            (vec![0, 0, 0, 1], vec![0x00, b'b', b'l', b'u', b'e']),
        ]
    );
    let row = |id| {
        let storage = storage.clone();
        async move {
            storage
                .get("tags".to_owned(), row_key(id))
                .await
                .unwrap()
                .unwrap()
        }
    };
    assert_eq!(
        row(1).await,
        vec![
            0x00, // variant tag
            1, 0, 0, 0, 0, 0, 0, 0, // id = 1
            0, 0, 0, 0, // tag = code 0 ("red")
            0, 0, 0, 0, 0, // alias = null
        ]
    );
    assert_eq!(
        row(2).await,
        vec![
            0x00, // variant tag
            2, 0, 0, 0, 0, 0, 0, 0, // id = 2
            1, 0, 0, 0, // tag = code 1 ("blue")
            1, 0, 0, 0, 0, // alias = present, code 0 ("red")
        ]
    );
}
