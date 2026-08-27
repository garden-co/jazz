//! Manual A/B receipt kept on compatibility APIs so the identical source can
//! run against both the fixed-u64 A commit and generic-varint B commit.

use groove::db::{Database, GraphBuilder, ProjectField};
use groove::records::{RecordDescriptor, Value, VariantRecord};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

const ROWS: u64 = 20_000;
const REPS: usize = 7;

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "entries",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("owner", ColumnType::U64),
            ColumnSchema::new("body", ColumnType::String),
            ColumnSchema::new("extra", ColumnType::Bool),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("entries_by_owner", ["owner"]))
    .with_variant(1, ["id", "owner", "body"])
    .with_variant(2, ["id", "owner", "body", "extra"])
    .with_variant(3, ["id", "owner", "body"])
    .with_variant(4, ["id", "owner", "body", "extra"])])
}

fn register_projection(database: &mut Database) -> Result<(), groove::db::Error> {
    database.define_variant_projection(
        "entries",
        "receipt",
        RecordDescriptor::new([
            ("id", ColumnType::U64),
            ("kind", ColumnType::String),
            ("label", ColumnType::String),
        ]),
    )?;
    for tag in 1..=4 {
        database.register_variant_projection_case(
            "entries",
            "receipt",
            tag,
            [
                ProjectField::named("id"),
                ProjectField::literal(
                    "kind",
                    Value::String(if tag % 2 == 0 { "even" } else { "odd" }.into()),
                ),
                ProjectField::renamed("body", "label"),
            ],
        )?;
    }
    Ok(())
}

#[futures_test::test]
#[ignore = "#1787: manual release A/B receipt"]
async fn repeated_release_write_ivm_and_cold_scan_receipt() -> Result<(), Box<dyn std::error::Error>>
{
    let descriptors = {
        let schema = schema();
        (1..=4)
            .map(|tag| {
                schema
                    .table("entries")
                    .unwrap()
                    .record_schema_for_variant(tag)
                    .unwrap()
            })
            .collect::<Vec<_>>()
    };
    let mut commits = Vec::with_capacity(REPS);
    let mut scans = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let database_schema = schema();
        let storage = MemoryStorage::new(&database_schema.column_families())
            .expect("valid memory storage families");
        let mut database = Database::new(database_schema, storage).await?;
        register_projection(&mut database)?;
        let subscription = database
            .subscribe_one_sink(GraphBuilder::variant_source("entries", "receipt"))
            .await?;
        assert!(subscription.recv()?.is_empty());
        let started = std::time::Instant::now();
        let mut batch = database.open_batch();
        for id in 0..ROWS {
            let tag = (id % 4 + 1) as u32;
            let mut values = vec![
                Value::U64(id),
                Value::U64(id % 100),
                Value::String(format!("value-{id}")),
            ];
            if tag.is_multiple_of(2) {
                values.push(Value::Bool(id % 2 == 0));
            }
            batch.insert(
                "entries",
                VariantRecord::create(tag, descriptors[tag as usize - 1], &values)?,
            );
        }
        let applied = database.apply_batch(batch).await?;
        let persisted = applied.persist().await;
        database.finish_persistence(persisted)?;
        drop(applied);
        let delivered = subscription.recv()?;
        assert_eq!(delivered.deltas.len(), ROWS as usize);
        commits.push(started.elapsed().as_micros());
        drop(subscription);

        let storage = database.into_storage();
        let mut reopened = Database::new(schema(), storage).await?;
        register_projection(&mut reopened)?;
        let scan_started = std::time::Instant::now();
        let hydration = reopened
            .subscribe_one_sink(GraphBuilder::variant_source("entries", "receipt"))
            .await?
            .recv()?;
        assert_eq!(hydration.deltas.len(), ROWS as usize);
        scans.push(scan_started.elapsed().as_micros());
    }
    eprintln!("receipt rows={ROWS} commit_us={commits:?} cold_scan_us={scans:?}");
    Ok(())
}
