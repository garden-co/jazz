use groove::db::{Database, GraphBuilder, ProjectField};
use groove::records::{
    RecordDescriptor, Value, VariantRecord, encode_variant_record, split_variant_record,
};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

fn union_schema() -> DatabaseSchema {
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
    // and two user-declared union cases. The tag is deliberately opaque to
    // Groove; Jazz retains the semantic pair in its catalogue.
    .with_variant(1, ["id", "owner", "body"])
    .with_variant(2, ["id", "owner", "url"])
    .with_variant(3, ["id", "owner", "body", "edited"])
    .with_variant(4, ["id", "owner", "url", "alt"])])
}

fn variant_row(tag: u32, values: &[Value]) -> VariantRecord {
    let schema = union_schema();
    let descriptor = schema
        .table("entries")
        .unwrap()
        .record_schema_for_variant(tag)
        .unwrap();
    VariantRecord::create(u64::from(tag), descriptor, values).unwrap()
}

#[test]
fn user_union_nested_in_layout_union_normalizes_immediately()
-> Result<(), Box<dyn std::error::Error>> {
    let schema = union_schema();
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage)?;
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

    let subscription =
        database.subscribe_one_sink(GraphBuilder::variant_project("entries", "public-entry"))?;
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
    database.commit_batch(batch)?;

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

/// Manual receipt for comparing the generic-union spelling with the existing
/// schema-version path. The IVM path is intentionally the same implementation;
/// this exercises write, index maintenance, immediate projection, and delivery
/// together while the codec test above isolates the changed prefix.
#[test]
#[ignore = "manual performance receipt"]
fn measure_variant_write_projection_and_index_path() -> Result<(), Box<dyn std::error::Error>> {
    const ROWS: u64 = 20_000;
    let schema = union_schema();
    let descriptors = (1..=4)
        .map(|tag| {
            schema
                .table("entries")
                .unwrap()
                .record_schema_for_variant(tag)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage)?;
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
    let subscription =
        database.subscribe_one_sink(GraphBuilder::variant_project("entries", "receipt"))?;
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
            VariantRecord::create(tag as u64, descriptors[tag - 1].clone(), &values)?,
        );
    }
    database.commit_batch(batch)?;
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
    let schema = union_schema();
    let mut reopened = Database::new(schema, storage)?;
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
        .subscribe_one_sink(GraphBuilder::variant_project("entries", "receipt"))?
        .recv()?;
    assert_eq!(hydration.deltas.len(), ROWS as usize);
    eprintln!(
        "variant receipt: reopen_hydration_ms={} rows={ROWS}",
        scan_started.elapsed().as_millis(),
    );
    Ok(())
}
