use groove::db::{Database, Error, GraphBuilder, IvmRuntimeError, ProjectField};
use groove::records::{RecordDescriptor, Value, ValueType, VariantRecord};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
    TableVariant,
};
use groove::storage::{MemoryStorage, ResidentStorage};

fn versioned_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("completed", ColumnType::Bool),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    // A version's logical field order is independent from catalogue order.
    .with_variant(1, ["title", "id"])
    .with_variant(2, ["id", "title", "completed"])])
}

fn open_database() -> Result<Database<MemoryStorage>, Error> {
    let schema = versioned_schema();
    let storage = MemoryStorage::new(&schema.column_families());
    Database::new(schema, storage)
}

fn row(version: u32, values: &[Value]) -> VariantRecord {
    let schema = versioned_schema();
    let descriptor = schema
        .table("items")
        .unwrap()
        .record_schema_for_variant(version)
        .expect("registered test version");
    VariantRecord::create(version, descriptor, values).expect("valid test row")
}

fn row_for(
    schema: &DatabaseSchema,
    version: u32,
    values: &[Value],
) -> Result<VariantRecord, groove::records::Error> {
    let descriptor = schema
        .table("items")
        .unwrap()
        .record_schema_for_variant(version)
        .expect("registered test version");
    VariantRecord::create(version, descriptor, values)
}

#[test]
fn active_variant_projection_accepts_an_appended_case_without_rebuilding()
-> Result<(), Box<dyn std::error::Error>> {
    let table = TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("completed", ColumnType::Bool),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_variant(1, ["title", "id"]);
    let schema = DatabaseSchema::new([table]);
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage)?;
    let output = RecordDescriptor::new([("id", ValueType::U64), ("title", ValueType::String)]);
    database.define_variant_projection("items", "reader-v1", output)?;
    database.register_variant_projection_case(
        "items",
        "reader-v1",
        1,
        [ProjectField::named("id"), ProjectField::named("title")],
    )?;

    let subscription =
        database.subscribe_one_sink(GraphBuilder::variant_source("items", "reader-v1"))?;
    let subscription_id = subscription.id();
    assert!(subscription.recv()?.is_empty());

    let v1 = RecordDescriptor::new([("title", ValueType::String), ("id", ValueType::U64)]);
    let mut batch = database.open_batch();
    batch.insert(
        "items",
        VariantRecord::create(1, v1, &[Value::String("first".into()), Value::U64(1)])?,
    );
    database.commit_batch(batch)?;
    assert_eq!(
        subscription.recv()?.to_values()?,
        vec![(vec![Value::U64(1), Value::String("first".into())], 1,)]
    );

    database.register_table_variant("items", TableVariant::new(2, ["id", "title", "completed"]))?;
    database.register_variant_projection_case(
        "items",
        "reader-v1",
        2,
        [ProjectField::named("id"), ProjectField::named("title")],
    )?;
    assert_eq!(subscription.id(), subscription_id);
    assert!(subscription.try_recv().is_err());

    let v2 = RecordDescriptor::new([
        ("id", ValueType::U64),
        ("title", ValueType::String),
        ("completed", ValueType::Bool),
    ]);
    let mut batch = database.open_batch();
    batch.update(
        "items",
        VariantRecord::create(
            2,
            v2,
            &[
                Value::U64(1),
                Value::String("first, revised".into()),
                Value::Bool(true),
            ],
        )?,
    );
    database.commit_batch(batch)?;

    let mut deltas = subscription.recv()?.to_values()?;
    deltas.sort_by_key(|(_, weight)| *weight);
    assert_eq!(
        deltas,
        vec![
            (vec![Value::U64(1), Value::String("first".into())], -1,),
            (
                vec![Value::U64(1), Value::String("first, revised".into())],
                1,
            ),
        ]
    );
    assert_eq!(
        database
            .query_graph(GraphBuilder::variant_source("items", "reader-v1"))?
            .to_values()?,
        vec![(
            vec![Value::U64(1), Value::String("first, revised".into())],
            1,
        )]
    );
    Ok(())
}

#[test]
fn ignored_variant_projection_case_is_distinct_from_an_unregistered_case()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = versioned_schema();
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema.clone(), storage)?;
    let output = RecordDescriptor::new([("id", ValueType::U64), ("title", ValueType::String)]);
    database.define_variant_projection("items", "v1-only", output)?;
    database.register_variant_projection_case(
        "items",
        "v1-only",
        1,
        [ProjectField::named("id"), ProjectField::named("title")],
    )?;
    database.register_variant_projection_ignore_case("items", "v1-only", 2)?;

    let subscription =
        database.subscribe_one_sink(GraphBuilder::variant_source("items", "v1-only"))?;
    assert!(subscription.recv()?.is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "items",
        row_for(
            &schema,
            2,
            &[
                Value::U64(2),
                Value::String("second".into()),
                Value::Bool(true),
            ],
        )?,
    );
    database.commit_batch(batch)?;
    assert!(subscription.try_recv().is_err());
    assert!(
        database
            .query_graph(GraphBuilder::variant_source("items", "v1-only"))?
            .is_empty()
    );

    database.define_variant_projection("items", "unregistered", output)?;
    assert!(matches!(
        database.query_graph(GraphBuilder::variant_source("items", "unregistered")),
        Err(Error::IvmRuntime(
            IvmRuntimeError::VariantProjectionCaseNotFound { version: 2, .. }
        ))
    ));
    Ok(())
}

fn indexed_versioned_schema(unique_email: bool) -> DatabaseSchema {
    let email_index = IndexSchema::new("items_by_email", ["email"]);
    let email_index = if unique_email {
        email_index.unique()
    } else {
        email_index
    };
    DatabaseSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("email", ColumnType::String),
            ColumnSchema::new("active", ColumnType::Bool),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(email_index)
    .with_index(IndexSchema::new("items_by_active", ["active"]))
    .with_variant(1, ["email", "id"])
    .with_variant(2, ["id", "email", "active"])])
}

#[test]
fn variant_indices_span_versions_skip_missing_fields_and_survive_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = indexed_versioned_schema(false);
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema.clone(), storage)?;
    let active_subscription =
        database.subscribe_one_sink(GraphBuilder::index("items", "items_by_active"))?;
    assert!(active_subscription.recv()?.is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "items",
        row_for(
            &schema,
            1,
            &[Value::String("first@example.com".into()), Value::U64(1)],
        )?,
    );
    batch.insert(
        "items",
        row_for(
            &schema,
            2,
            &[
                Value::U64(2),
                Value::String("second@example.com".into()),
                Value::Bool(true),
            ],
        )?,
    );
    database.commit_batch(batch)?;

    let active_delta = active_subscription.recv()?.to_values()?;
    assert_eq!(active_delta.len(), 1);
    assert_eq!(active_delta[0].1, 1);
    let first = database.index_get(
        "items",
        "items_by_email",
        &[Value::String("first@example.com".into())],
    )?;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].variant_tag(), 1);
    assert_eq!(
        database
            .index_get("items", "items_by_active", &[Value::Bool(true)])?
            .len(),
        1
    );

    let mut batch = database.open_batch();
    batch.update(
        "items",
        row_for(
            &schema,
            2,
            &[
                Value::U64(1),
                Value::String("first+new@example.com".into()),
                Value::Bool(true),
            ],
        )?,
    );
    batch.update(
        "items",
        row_for(
            &schema,
            1,
            &[
                Value::String("second+old@example.com".into()),
                Value::U64(2),
            ],
        )?,
    );
    database.commit_batch(batch)?;

    let mut active_deltas = active_subscription.recv()?.to_values()?;
    active_deltas.sort_by_key(|(_, weight)| *weight);
    assert_eq!(active_deltas.len(), 2);
    assert_eq!(active_deltas[0].1, -1);
    assert_eq!(active_deltas[1].1, 1);
    assert!(
        database
            .index_get(
                "items",
                "items_by_email",
                &[Value::String("first@example.com".into())],
            )?
            .is_empty()
    );
    assert_eq!(
        database
            .index_get("items", "items_by_active", &[Value::Bool(true)])?
            .len(),
        1
    );

    let storage = database.into_storage();
    let reopened = Database::new(schema, storage)?;
    let second = reopened.index_get(
        "items",
        "items_by_email",
        &[Value::String("second+old@example.com".into())],
    )?;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].variant_tag(), 1);
    assert_eq!(
        reopened
            .index_get("items", "items_by_active", &[Value::Bool(true)])?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn unique_variant_index_rejects_conflicts_across_versions() -> Result<(), Box<dyn std::error::Error>>
{
    let schema = indexed_versioned_schema(true);
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema.clone(), storage)?;
    let mut batch = database.open_batch();
    batch.insert(
        "items",
        row_for(
            &schema,
            1,
            &[Value::String("same@example.com".into()), Value::U64(1)],
        )?,
    );
    database.commit_batch(batch)?;

    let mut batch = database.open_batch();
    batch.insert(
        "items",
        row_for(
            &schema,
            2,
            &[
                Value::U64(2),
                Value::String("same@example.com".into()),
                Value::Bool(true),
            ],
        )?,
    );
    assert!(matches!(
        database.commit_batch(batch),
        Err(Error::IvmRuntime(
            IvmRuntimeError::UniqueIndexViolation { .. }
        ))
    ));
    Ok(())
}

#[test]
fn active_variant_index_accepts_a_live_schema_version_without_rebuilding()
-> Result<(), Box<dyn std::error::Error>> {
    let table = TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("email", ColumnType::String),
            ColumnSchema::new("active", ColumnType::Bool),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("items_by_active", ["active"]))
    .with_variant(1, ["email", "id"]);
    let schema = DatabaseSchema::new([table]);
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage)?;
    let subscription =
        database.subscribe_one_sink(GraphBuilder::index("items", "items_by_active"))?;
    let subscription_id = subscription.id();
    assert!(subscription.recv()?.is_empty());

    database.register_table_variant("items", TableVariant::new(2, ["id", "email", "active"]))?;
    assert_eq!(subscription.id(), subscription_id);

    let descriptor = RecordDescriptor::new([
        ("id", ValueType::U64),
        ("email", ValueType::String),
        ("active", ValueType::Bool),
    ]);
    let mut batch = database.open_batch();
    batch.insert(
        "items",
        VariantRecord::create(
            2,
            descriptor,
            &[
                Value::U64(1),
                Value::String("new@example.com".into()),
                Value::Bool(true),
            ],
        )?,
    );
    database.commit_batch(batch)?;

    let deltas = subscription.recv()?.to_values()?;
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].1, 1);
    assert_eq!(
        database
            .index_get("items", "items_by_active", &[Value::Bool(true)])?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn live_variant_index_backfills_existing_rows_without_perturbing_subscriptions()
-> Result<(), Box<dyn std::error::Error>> {
    let table = TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("email", ColumnType::String),
            ColumnSchema::new("active", ColumnType::Bool),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_variant(1, ["email", "id"])
    .with_variant(2, ["id", "email", "active"]);
    let schema = DatabaseSchema::new([table.clone()]);
    let mut column_families = schema.column_families();
    column_families.push("indices");
    let storage = MemoryStorage::new(&column_families);
    let mut database = Database::new(schema.clone(), storage)?;
    let projection = RecordDescriptor::new([("id", ValueType::U64), ("email", ValueType::String)]);
    database.define_variant_projection("items", "reader", projection)?;
    for version in [1, 2] {
        database.register_variant_projection_case(
            "items",
            "reader",
            version,
            [ProjectField::named("id"), ProjectField::named("email")],
        )?;
    }
    let subscription =
        database.subscribe_one_sink(GraphBuilder::variant_source("items", "reader"))?;
    assert!(subscription.recv()?.is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "items",
        row_for(
            &schema,
            1,
            &[Value::String("old@example.com".into()), Value::U64(1)],
        )?,
    );
    batch.insert(
        "items",
        row_for(
            &schema,
            2,
            &[
                Value::U64(2),
                Value::String("new@example.com".into()),
                Value::Bool(true),
            ],
        )?,
    );
    database.commit_batch(batch)?;
    assert_eq!(subscription.recv()?.deltas.len(), 2);

    let index = IndexSchema::new("items_by_active", ["active"]);
    database.register_table_index("items", index.clone())?;
    assert!(subscription.try_recv().is_err());
    let indexed = database.index_get("items", "items_by_active", &[Value::Bool(true)])?;
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].variant_tag(), 2);
    assert_eq!(indexed[0].get("id")?, Value::U64(2));

    let mut batch = database.open_batch();
    batch.update(
        "items",
        row_for(
            &schema,
            2,
            &[
                Value::U64(2),
                Value::String("new@example.com".into()),
                Value::Bool(false),
            ],
        )?,
    );
    database.commit_batch(batch)?;
    assert!(
        database
            .index_get("items", "items_by_active", &[Value::Bool(true)])?
            .is_empty()
    );
    assert_eq!(
        database
            .index_get("items", "items_by_active", &[Value::Bool(false)])?
            .len(),
        1
    );

    let storage = database.into_storage();
    let reopened_schema = DatabaseSchema::new([table.with_index(index)]);
    let reopened = Database::new(reopened_schema, storage)?;
    assert_eq!(
        reopened
            .index_get("items", "items_by_active", &[Value::Bool(false)])?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn mixed_schema_versions_survive_replacement_and_reopen() -> Result<(), Box<dyn std::error::Error>>
{
    let mut database = open_database()?;
    let mut batch = database.open_batch();
    batch.insert(
        "items",
        row(1, &[Value::String("first".into()), Value::U64(1)]),
    );
    batch.insert(
        "items",
        row(
            2,
            &[
                Value::U64(2),
                Value::String("second".into()),
                Value::Bool(false),
            ],
        ),
    );
    database.commit_batch(batch)?;

    let rows = database.primary_key_scan("items", &[])?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].variant_tag(), 1);
    assert_eq!(rows[0].get("title")?, Value::String("first".into()));
    assert!(rows[0].get("completed").is_err());
    assert_eq!(rows[1].variant_tag(), 2);
    assert_eq!(rows[1].get("completed")?, Value::Bool(false));

    let mut batch = database.open_batch();
    batch.update(
        "items",
        row(
            2,
            &[
                Value::U64(1),
                Value::String("first, revised".into()),
                Value::Bool(true),
            ],
        ),
    );
    database.commit_batch(batch)?;
    let replaced = database
        .primary_key_get("items", &[Value::U64(1)])?
        .expect("updated row exists");
    assert_eq!(replaced.variant_tag(), 2);
    assert_eq!(
        replaced.get("title")?,
        Value::String("first, revised".into())
    );
    assert_eq!(replaced.get("completed")?, Value::Bool(true));

    let storage = database.into_storage();
    let reopened = Database::new(versioned_schema(), storage)?;
    let rows = reopened.primary_key_scan("items", &[])?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].variant_tag(), 2);
    assert_eq!(rows[1].variant_tag(), 2);
    assert_eq!(rows[1].get("completed")?, Value::Bool(false));
    Ok(())
}

#[test]
fn variant_header_is_canonical_varint_and_validated_on_read()
-> Result<(), Box<dyn std::error::Error>> {
    let mut database = open_database()?;
    let record = row(1, &[Value::String("first".into()), Value::U64(1)]);
    let payload_len = record.raw().len();
    let mut batch = database.open_batch();
    batch.insert("items", record);
    database.commit_batch(batch)?;

    let storage = database.into_storage();
    let entries = storage.prefix("items", b"")?;
    let (key, stored) = entries.first().expect("stored row");
    assert_eq!(stored[0], 1);
    assert_eq!(stored.len(), payload_len + 1);

    storage.set("items", key, &[0x80, 0x00])?;
    let reopened = Database::new(versioned_schema(), storage)?;
    assert!(matches!(
        reopened.primary_key_get("items", &[Value::U64(1)]),
        Err(Error::RecordEncoding(
            groove::records::Error::InvalidSchemaVersionHeader
        ))
    ));
    Ok(())
}

#[test]
fn writes_reject_unregistered_schema_versions() -> Result<(), Box<dyn std::error::Error>> {
    let mut database = open_database()?;
    let mut batch = database.open_batch();
    batch.insert(
        "items",
        VariantRecord::new(
            3,
            groove::records::OwnedRecord::new(
                Vec::new(),
                versioned_schema().table("items").unwrap().record_schema(),
            ),
        ),
    );
    assert!(matches!(
        database.commit_batch(batch),
        Err(Error::UnknownTableVariant { table, version })
            if table == "items" && version == 3
    ));
    Ok(())
}

#[test]
fn explicit_registries_cannot_claim_reserved_version_zero() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "items",
        [ColumnSchema::new("id", ColumnType::U64)],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_variant(0, ["id"])]);
    let storage = MemoryStorage::new(&schema.column_families());
    assert!(matches!(
        Database::new(schema, storage),
        Err(Error::ReservedTableVariant(table)) if table == "items"
    ));
}
