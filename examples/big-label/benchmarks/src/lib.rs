//! Self-contained BigLabel fixture and representative read workloads.
//!
//! This deliberately duplicates the small schema surface needed by the
//! benchmark. It does not import an application runtime or fixture helper.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, MergeableTxOps, PreparedQuery, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

const LABELS: usize = 8;
const ARTISTS: usize = 32;
const CATALOGS: usize = 4;

type BenchDb = Db<MemoryStorage>;

/// Prepared BigLabel fixture. Construction and seeding are intentionally kept
/// outside the measured closure.
pub struct Fixture {
    db: BenchDb,
    release_table: TableSchema,
    label_load: PreparedQuery,
    artist_load: PreparedQuery,
    catalog_load: PreparedQuery,
}

impl Fixture {
    pub fn new(release_count: usize) -> Self {
        assert!(release_count > 0, "fixture requires at least one release");
        let (db, release_table) = open_db();

        seed_labels_and_artists(&db);
        for release in 0..release_count {
            insert(&db, "releases", row_id(4, release), release_cells(release));
        }

        let label_load = prepare_release_load(&db, "label", row_id(1, 0));
        let artist_load = prepare_release_load(&db, "artist", row_id(2, 0));
        let catalog_load = prepare_release_load(&db, "catalog", row_id(3, 0));
        Self {
            db,
            release_table,
            label_load,
            artist_load,
            catalog_load,
        }
    }

    pub fn label_load(&self) -> usize {
        self.read_count(&self.label_load)
    }

    pub fn artist_load(&self) -> usize {
        self.read_count(&self.artist_load)
    }

    pub fn catalog_load(&self) -> usize {
        self.read_count(&self.catalog_load)
    }

    pub fn label_release_order(&self) -> Vec<u64> {
        self.release_order(&self.label_load)
    }

    fn read_count(&self, query: &PreparedQuery) -> usize {
        self.db
            .read(query)
            .expect("BigLabel benchmark read succeeds")
            .len()
    }

    fn release_order(&self, query: &PreparedQuery) -> Vec<u64> {
        self.db
            .read(query)
            .expect("BigLabel benchmark read succeeds")
            .into_iter()
            .map(|row| match row.cell(&self.release_table, "released_at") {
                Some(Value::U64(released_at)) => released_at,
                other => panic!("release has unexpected released_at value: {other:?}"),
            })
            .collect()
    }
}

/// An opened BigLabel database with its small dimension tables seeded outside
/// the measured ingest operation.
pub struct IngestFixture {
    db: BenchDb,
    release_table: TableSchema,
}

impl IngestFixture {
    pub fn new() -> Self {
        let (db, release_table) = open_db();
        seed_labels_and_artists(&db);
        Self { db, release_table }
    }

    pub fn ingest_releases(&self, release_count: usize, batch_size: usize) -> usize {
        assert!(batch_size > 0, "ingest batch size must be non-zero");
        seed_releases_with_batch_size(&self.db, release_count, batch_size);
        release_count
    }

    pub fn release_count(&self) -> usize {
        let query = self
            .db
            .prepare_query(&Query::from("releases"))
            .expect("prepare all BigLabel releases");
        self.db
            .read(&query)
            .expect("read all BigLabel releases")
            .len()
    }

    /// Return one label's imported releases via its indexed relationship,
    /// newest release first. This is an untimed correctness oracle for the
    /// ingest benchmarks, not part of the measured closure.
    pub fn label_release_titles_and_order(&self, label: usize) -> Vec<(String, u64)> {
        let query = prepare_release_load(&self.db, "label", row_id(1, label));
        self.db
            .read(&query)
            .expect("read imported releases by label")
            .into_iter()
            .map(|row| {
                let title = match row.cell(&self.release_table, "title") {
                    Some(Value::String(title)) => title,
                    other => panic!("release has unexpected title value: {other:?}"),
                };
                let released_at = match row.cell(&self.release_table, "released_at") {
                    Some(Value::U64(released_at)) => released_at,
                    other => panic!("release has unexpected released_at value: {other:?}"),
                };
                (title, released_at)
            })
            .collect()
    }
}

impl Default for IngestFixture {
    fn default() -> Self {
        Self::new()
    }
}

pub fn expected_counts(release_count: usize) -> (usize, usize, usize) {
    (
        release_count.div_ceil(LABELS),
        release_count.div_ceil(ARTISTS),
        release_count.div_ceil(CATALOGS),
    )
}

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("labels")
                .column("name", ColumnType::Text)
                .fk_column("catalog", "catalogs")
                .index_only(["catalog"]),
        )
        .table(
            TableSchemaBuilder::new("artists")
                .column("name", ColumnType::Text)
                .fk_column("label", "labels")
                .index_only(["label"]),
        )
        .table(TableSchemaBuilder::new("catalogs").column("name", ColumnType::Text))
        .table(
            TableSchemaBuilder::new("releases")
                .column("title", ColumnType::Text)
                .fk_column("label", "labels")
                .fk_column("artist", "artists")
                .fk_column("catalog", "catalogs")
                .column("released_at", ColumnType::Timestamp)
                .index_only(["label", "artist", "catalog", "released_at"]),
        )
        .build();
    JazzSchema::new(&source).expect("BigLabel benchmark schema compiles")
}

fn open_db() -> (BenchDb, TableSchema) {
    let schema = schema();
    let release_table = schema
        .tables()
        .iter()
        .find(|table| table.name == "releases")
        .expect("BigLabel schema has releases")
        .clone();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        MemoryStorage::new(&family_refs).expect("valid memory storage families"),
        DbIdentity {
            node: NodeUuid::from_bytes([0xb1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open BigLabel benchmark database");

    for catalog in 0..CATALOGS {
        insert(
            &db,
            "catalogs",
            row_id(3, catalog),
            BTreeMap::from([(
                "name".to_owned(),
                Value::String(format!("Catalog {catalog:02}")),
            )]),
        );
    }
    (db, release_table)
}

fn insert(db: &BenchDb, table: &str, id: RowUuid, cells: BTreeMap<String, Value>) {
    let write = block_on(db.insert(
        table,
        cells,
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .expect("insert BigLabel fixture row");
    block_on(write.wait(DurabilityTier::Local)).expect("fixture row reaches local durability");
}

fn seed_labels_and_artists(db: &BenchDb) {
    for label in 0..LABELS {
        insert(
            db,
            "labels",
            row_id(1, label),
            BTreeMap::from([
                (
                    "name".to_owned(),
                    Value::String(format!("Label {label:02}")),
                ),
                (
                    "catalog".to_owned(),
                    Value::Uuid(row_id(3, label % CATALOGS).0),
                ),
            ]),
        );
    }
    for artist in 0..ARTISTS {
        insert(
            db,
            "artists",
            row_id(2, artist),
            BTreeMap::from([
                (
                    "name".to_owned(),
                    Value::String(format!("Artist {artist:03}")),
                ),
                (
                    "label".to_owned(),
                    Value::Uuid(row_id(1, artist % LABELS).0),
                ),
            ]),
        );
    }
}

fn seed_releases_with_batch_size(db: &BenchDb, release_count: usize, batch_size: usize) {
    for start in (0..release_count).step_by(batch_size) {
        let end = (start + batch_size).min(release_count);
        block_on(db.transaction(async |tx| {
            for release in start..end {
                tx.insert("releases", release_cells(release), InsertOptions::default())
                    .await?;
            }
            Ok(())
        }))
        .unwrap_or_else(|error| panic!("seed BigLabel releases {start}..{end}: {error}"));
    }
}

fn release_cells(release: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "title".to_owned(),
            Value::String(format!("Release {release:06}")),
        ),
        (
            "label".to_owned(),
            Value::Uuid(row_id(1, release % LABELS).0),
        ),
        (
            "artist".to_owned(),
            Value::Uuid(row_id(2, release % ARTISTS).0),
        ),
        (
            "catalog".to_owned(),
            Value::Uuid(row_id(3, release % CATALOGS).0),
        ),
        ("released_at".to_owned(), Value::U64(release as u64)),
    ])
}

fn prepare_release_load(db: &BenchDb, column: &str, id: RowUuid) -> PreparedQuery {
    db.prepare_query(
        &Query::from("releases")
            .filter(eq(col(column), lit(id.0)))
            .order_by("released_at", OrderDirection::Desc),
    )
    .expect("prepare BigLabel release load")
}

fn row_id(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}
