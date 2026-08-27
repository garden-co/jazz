use groove::db::{Database, GraphBuilder, ProjectField};
use groove::records::{
    EnumCase, EnumSchema, RecordDescriptor, Value, ValueType, VariantRecord, encode_variant_record,
    split_variant_record,
};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
    TableVariantField,
};
use groove::storage::MemoryStorage;

fn enum_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "entries",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("owner", ColumnType::U64),
            ColumnSchema::new("body", ColumnType::String),
            ColumnSchema::new("url", ColumnType::String),
            ColumnSchema::new("edited", ColumnType::Bool),
            ColumnSchema::new("alt", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("entries_by_owner", ["owner"]))
    // The four physical cases are the product of two Jazz storage layouts
    // and two user-declared enum cases. The tag is deliberately opaque to
    // Groove; Jazz retains the semantic pair in its catalogue.
    .with_variant(1, ["id", "owner", "body"])
    .with_variant(2, ["id", "owner", "url"])
    .with_variant(3, ["id", "owner", "body", "edited"])
    .with_variant(4, ["id", "owner", "url", "alt"])])
}

fn enum_projection_schema() -> DatabaseSchema {
    let table = TableSchema::new("events", [ColumnSchema::new("id", ColumnType::U64)])
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
        // Two layout generations × two logical cases. `value` is deliberately
        // case-local: its type differs between text and metric cases.
        .with_variant_payload(
            1,
            [
                TableVariantField::shared("id", ColumnType::U64, "id"),
                TableVariantField::local("value", ColumnType::String),
            ],
        )
        .with_variant_payload(
            2,
            [
                TableVariantField::shared("id", ColumnType::U64, "id"),
                TableVariantField::local("value", ColumnType::U64),
            ],
        )
        .with_variant_payload(
            3,
            [
                TableVariantField::shared("id", ColumnType::U64, "id"),
                TableVariantField::local("value", ColumnType::String),
                TableVariantField::local("layout_note", ColumnType::Bool),
            ],
        )
        .with_variant_payload(
            4,
            [
                TableVariantField::shared("id", ColumnType::U64, "id"),
                TableVariantField::local("value", ColumnType::U64),
                TableVariantField::local("layout_note", ColumnType::Bool),
            ],
        );
    DatabaseSchema::new([table])
}

#[futures_test::test]
async fn variant_enum_projection_normalizes_layout_tags_and_matches_named_case()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = enum_projection_schema();
    let storage =
        MemoryStorage::new(&schema.column_families()).expect("valid memory storage families");
    let mut database = Database::new(schema.clone(), storage).await?;
    let event = EnumSchema::new(
        "event",
        [
            EnumCase::new(
                "text",
                RecordDescriptor::new([("value", ValueType::String)]),
            ),
            EnumCase::new("metric", RecordDescriptor::new([("value", ValueType::U64)])),
        ],
    )?;
    let normalized = RecordDescriptor::new([("event", ValueType::Enum(Box::new(event.clone())))]);
    database.define_variant_projection("events", "logical-event", normalized)?;
    for (tag, case) in [(1, "text"), (2, "metric"), (3, "text"), (4, "metric")] {
        database.register_variant_enum_case(
            "events",
            "logical-event",
            tag,
            "event",
            &event,
            case,
            [ProjectField::named("value")],
        )?;
    }

    let projected = GraphBuilder::variant_source("events", "logical-event");
    let subscription = database
        .subscribe_one_sink(projected.clone().variant_project("event", "text"))
        .await?;
    assert!(subscription.recv()?.is_empty());

    let descriptors = (1..=4)
        .map(|tag| {
            schema
                .table("events")
                .unwrap()
                .record_schema_for_variant(tag)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut batch = database.open_batch();
    batch.insert(
        "events",
        VariantRecord::create(
            1,
            descriptors[0],
            &[Value::U64(1), Value::String("first".into())],
        )?,
    );
    batch.insert(
        "events",
        VariantRecord::create(2, descriptors[1], &[Value::U64(2), Value::U64(7)])?,
    );
    batch.insert(
        "events",
        VariantRecord::create(
            3,
            descriptors[2],
            &[
                Value::U64(3),
                Value::String("second".into()),
                Value::Bool(true),
            ],
        )?,
    );
    batch.insert(
        "events",
        VariantRecord::create(
            4,
            descriptors[3],
            &[Value::U64(4), Value::U64(9), Value::Bool(true)],
        )?,
    );
    let applied = database.apply_batch(batch).await?;
    let persisted = applied.persist().await;
    database.finish_persistence(persisted)?;
    drop(applied);

    let all = database.query_graph(projected.clone()).await?.to_values()?;
    assert_eq!(all.len(), 4);
    let mut tags = all
        .iter()
        .map(|(values, _)| match &values[0] {
            Value::Enum(value) => value.tag(),
            value => panic!("expected enum, got {value:?}"),
        })
        .collect::<Vec<_>>();
    tags.sort_unstable();
    assert_eq!(
        tags,
        vec![0, 0, 1, 1],
        "physical tags normalize to logical case tags"
    );
    let mut text_deltas = subscription.recv()?.to_values()?;
    text_deltas.sort_by(|(left, _), (right, _)| format!("{left:?}").cmp(&format!("{right:?}")));
    assert_eq!(
        text_deltas,
        vec![
            (vec![Value::String("first".into())], 1),
            (vec![Value::String("second".into())], 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.update(
        "events",
        VariantRecord::create(
            3,
            descriptors[2],
            &[
                Value::U64(3),
                Value::String("revised".into()),
                Value::Bool(false),
            ],
        )?,
    );
    let applied = database.apply_batch(batch).await?;
    let persisted = applied.persist().await;
    database.finish_persistence(persisted)?;
    drop(applied);
    let mut revised_deltas = subscription.recv()?.to_values()?;
    revised_deltas.sort_by_key(|(_, weight)| *weight);
    assert_eq!(
        revised_deltas,
        vec![
            (vec![Value::String("second".into())], -1),
            (vec![Value::String("revised".into())], 1),
        ]
    );
    Ok(())
}

#[futures_test::test]
async fn case_local_same_name_may_have_different_types_when_not_shared()
-> Result<(), Box<dyn std::error::Error>> {
    let table = TableSchema::new("events", [ColumnSchema::new("id", ColumnType::U64)])
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
        .with_index(IndexSchema::new("events_by_id", ["id"]))
        .with_variant_payload(
            1,
            [
                TableVariantField::shared("id", ColumnType::U64, "id"),
                TableVariantField::local("value", ColumnType::String),
            ],
        )
        .with_variant_payload(
            2,
            [
                TableVariantField::shared("event_id", ColumnType::U64, "id"),
                TableVariantField::local("value", ColumnType::U64),
            ],
        );
    let schema = DatabaseSchema::new([table]);
    let storage =
        MemoryStorage::new(&schema.column_families()).expect("valid memory storage families");
    let mut database = Database::new(schema.clone(), storage).await?;
    let v1 = schema
        .table("events")
        .unwrap()
        .record_schema_for_variant(1)
        .unwrap();
    let v2 = schema
        .table("events")
        .unwrap()
        .record_schema_for_variant(2)
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "events",
        VariantRecord::create(1, v1, &[Value::U64(1), Value::String("opened".into())])?,
    );
    batch.insert(
        "events",
        VariantRecord::create(2, v2, &[Value::U64(2), Value::U64(404)])?,
    );
    let applied = database.apply_batch(batch).await?;
    let persisted = applied.persist().await;
    database.finish_persistence(persisted)?;
    drop(applied);
    let rows = database.primary_key_scan("events", &[]).await?;
    assert_eq!(rows[0].get("value")?, Value::String("opened".into()));
    assert_eq!(rows[1].get("value")?, Value::U64(404));

    let storage = database.into_storage();
    let reopened = Database::new(schema, storage).await?;
    let indexed = reopened.index_scan("events", "events_by_id", &[]).await?;
    assert_eq!(
        indexed.len(),
        2,
        "shared id index spans both local payloads"
    );
    assert_eq!(reopened.primary_key_scan("events", &[]).await?.len(), 2);
    Ok(())
}

fn variant_row(tag: u32, values: &[Value]) -> VariantRecord {
    let schema = enum_schema();
    let descriptor = schema
        .table("entries")
        .unwrap()
        .record_schema_for_variant(tag)
        .unwrap();
    VariantRecord::create(tag, descriptor, values).unwrap()
}

#[futures_test::test]
async fn user_enum_nested_in_layout_enum_normalizes_immediately()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = enum_schema();
    let storage =
        MemoryStorage::new(&schema.column_families()).expect("valid memory storage families");
    let mut database = Database::new(schema, storage).await?;
    let normalized = RecordDescriptor::new([
        ("id", ColumnType::U64),
        ("kind", ColumnType::String),
        ("label", ColumnType::String),
    ]);
    database.define_variant_projection("entries", "public-entry", normalized)?;
    database.register_variant_case(
        "entries",
        "public-entry",
        1,
        [
            ProjectField::named("id"),
            ProjectField::literal("kind", Value::String("text".into())),
            ProjectField::renamed("body", "label"),
        ],
    )?;
    database.register_variant_case(
        "entries",
        "public-entry",
        2,
        [
            ProjectField::named("id"),
            ProjectField::literal("kind", Value::String("image".into())),
            ProjectField::renamed("url", "label"),
        ],
    )?;
    database.register_variant_case(
        "entries",
        "public-entry",
        3,
        [
            ProjectField::named("id"),
            ProjectField::literal("kind", Value::String("text".into())),
            ProjectField::renamed("body", "label"),
        ],
    )?;
    database.register_variant_case(
        "entries",
        "public-entry",
        4,
        [
            ProjectField::named("id"),
            ProjectField::literal("kind", Value::String("image".into())),
            ProjectField::renamed("url", "label"),
        ],
    )?;

    let subscription = database
        .subscribe_one_sink(GraphBuilder::variant_source("entries", "public-entry"))
        .await?;
    assert!(subscription.recv()?.is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "entries",
        variant_row(
            1,
            &[Value::U64(1), Value::U64(7), Value::String("draft".into())],
        ),
    );
    batch.insert(
        "entries",
        variant_row(
            4,
            &[
                Value::U64(2),
                Value::U64(7),
                Value::String("/cover.png".into()),
                Value::String("cover".into()),
            ],
        ),
    );
    let applied = database.apply_batch(batch).await?;
    let persisted = applied.persist().await;
    database.finish_persistence(persisted)?;
    drop(applied);

    let mut rows = subscription.recv()?.to_values()?;
    rows.sort_by_key(|(values, _)| match values[0] {
        Value::U64(id) => id,
        _ => unreachable!("id projection is u64"),
    });
    assert_eq!(
        rows,
        vec![
            (
                vec![
                    Value::U64(1),
                    Value::String("text".into()),
                    Value::String("draft".into()),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(2),
                    Value::String("image".into()),
                    Value::String("/cover.png".into()),
                ],
                1,
            ),
        ]
    );
    Ok(())
}

#[test]
fn table_variant_tags_use_canonical_bounded_varints() {
    for (tag, expected_header_len) in [(0, 1), (127, 1), (128, 2), (16_383, 2), (16_384, 3)] {
        let stored = encode_variant_record(tag, b"payload");
        let (decoded, payload) = split_variant_record(&stored).unwrap();
        assert_eq!(decoded, tag);
        assert_eq!(payload, b"payload");
        assert_eq!(stored.len() - payload.len(), expected_header_len);
    }

    // Overlong and overflowing encodings are rejected, so one logical tag has
    // one durable byte representation.
    assert!(split_variant_record(&[0x80, 0x00]).is_err());
    assert!(split_variant_record(&[0xff, 0xff, 0xff, 0xff, 0x10]).is_err());
}

/// Manual receipt for comparing the generic-enum spelling with the existing
/// schema-version path. The IVM path is intentionally the same implementation;
/// this exercises write, index maintenance, immediate projection, and delivery
/// together while the codec test above isolates the changed prefix.
#[futures_test::test]
#[ignore = "#1787: manual performance receipt"]
async fn measure_variant_write_projection_and_index_path() -> Result<(), Box<dyn std::error::Error>>
{
    const ROWS: u64 = 20_000;
    let schema = enum_schema();
    let descriptors = (1..=4)
        .map(|tag| {
            schema
                .table("entries")
                .unwrap()
                .record_schema_for_variant(tag)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let storage =
        MemoryStorage::new(&schema.column_families()).expect("valid memory storage families");
    let mut database = Database::new(schema, storage).await?;
    let normalized = RecordDescriptor::new([
        ("id", ColumnType::U64),
        ("kind", ColumnType::String),
        ("label", ColumnType::String),
    ]);
    database.define_variant_projection("entries", "receipt", normalized)?;
    for (tag, kind, source) in [
        (1, "text", "body"),
        (2, "image", "url"),
        (3, "text", "body"),
        (4, "image", "url"),
    ] {
        database.register_variant_case(
            "entries",
            "receipt",
            tag,
            [
                ProjectField::named("id"),
                ProjectField::literal("kind", Value::String(kind.into())),
                ProjectField::renamed(source, "label"),
            ],
        )?;
    }
    let subscription = database
        .subscribe_one_sink(GraphBuilder::variant_source("entries", "receipt"))
        .await?;
    assert!(subscription.recv()?.is_empty());

    let started = std::time::Instant::now();
    let mut batch = database.open_batch();
    for id in 0..ROWS {
        let tag = (id % 4 + 1) as usize;
        let mut values = vec![
            Value::U64(id),
            Value::U64(id % 100),
            Value::String(format!("value-{id}")),
        ];
        if tag >= 3 {
            values.push(if tag == 3 {
                Value::Bool(id % 2 == 0)
            } else {
                Value::String(format!("alt-{id}"))
            });
        }
        batch.insert(
            "entries",
            VariantRecord::create(tag as u32, descriptors[tag - 1], &values)?,
        );
    }
    let applied = database.apply_batch(batch).await?;
    let persisted = applied.persist().await;
    database.finish_persistence(persisted)?;
    drop(applied);
    let committed = started.elapsed();
    let deltas = subscription.recv()?;
    let delivered = started.elapsed();
    assert_eq!(deltas.deltas.len(), ROWS as usize);
    eprintln!(
        "variant receipt: rows={ROWS} commit_ms={} delivered_ms={} tag_bytes_per_row=1",
        committed.as_millis(),
        delivered.as_millis(),
    );
    drop(subscription);
    let storage = database.into_storage();
    let schema = enum_schema();
    let mut reopened = Database::new(schema, storage).await?;
    let normalized = RecordDescriptor::new([
        ("id", ColumnType::U64),
        ("kind", ColumnType::String),
        ("label", ColumnType::String),
    ]);
    reopened.define_variant_projection("entries", "receipt", normalized)?;
    for (tag, kind, source) in [
        (1, "text", "body"),
        (2, "image", "url"),
        (3, "text", "body"),
        (4, "image", "url"),
    ] {
        reopened.register_variant_case(
            "entries",
            "receipt",
            tag,
            [
                ProjectField::named("id"),
                ProjectField::literal("kind", Value::String(kind.into())),
                ProjectField::renamed(source, "label"),
            ],
        )?;
    }
    let scan_started = std::time::Instant::now();
    let hydration = reopened
        .subscribe_one_sink(GraphBuilder::variant_source("entries", "receipt"))
        .await?
        .recv()?;
    assert_eq!(hydration.deltas.len(), ROWS as usize);
    eprintln!(
        "variant receipt: reopen_hydration_ms={} rows={ROWS}",
        scan_started.elapsed().as_millis(),
    );
    Ok(())
}
