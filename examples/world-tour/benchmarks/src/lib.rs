//! Self-contained WorldTour fixture and representative itinerary workloads.
//!
//! The benchmark intentionally duplicates the app's schema and query shapes;
//! it does not depend on a frontend runtime or app fixture helper.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, PreparedQuery, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, gte, lit, lte};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

type BenchDb = Db<MemoryStorage>;

/// A deterministic, in-memory WorldTour workload fixture.
pub struct Fixture {
    db: BenchDb,
    calendar_window: PreparedQuery,
    map_viewport: PreparedQuery,
}

impl Fixture {
    /// Seed `leg_count` dated itinerary legs across a predictable map grid.
    pub fn new(leg_count: usize) -> Self {
        assert!(leg_count >= 32, "fixture needs a useful itinerary window");
        let schema = schema();
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let db = block_on(Db::open(DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x57; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .expect("open WorldTour benchmark database");

        let tour = row_id(1, 0);
        insert(
            &db,
            "tours",
            tour,
            BTreeMap::from([("name".into(), Value::String("Summer world tour".into()))]),
        );
        for leg in 0..leg_count {
            let venue = row_id(2, leg);
            insert(
                &db,
                "venues",
                venue,
                BTreeMap::from([
                    ("name".into(), Value::String(format!("Venue {leg:05}"))),
                    (
                        "latitude".into(),
                        Value::F64(35.0 + (leg % 20) as f64 / 10.0),
                    ),
                    (
                        "longitude".into(),
                        Value::F64(-120.0 + (leg % 30) as f64 / 10.0),
                    ),
                    (
                        "time_zone".into(),
                        Value::String("America/Los_Angeles".into()),
                    ),
                ]),
            );
            insert(
                &db,
                "legs",
                row_id(3, leg),
                BTreeMap::from([
                    ("tour".into(), Value::Uuid(tour.0)),
                    ("venue".into(), Value::Uuid(venue.0)),
                    (
                        "starts_at".into(),
                        Value::U64(1_700_000_000 + leg as u64 * 86_400),
                    ),
                    ("status".into(), Value::String("confirmed".into())),
                ]),
            );
        }

        let calendar_window = db
            .prepare_query(
                &Query::from("legs")
                    .filter(eq(col("tour"), lit(tour.0)))
                    .filter(gte(col("starts_at"), lit(1_700_000_000_u64)))
                    .filter(lte(col("starts_at"), lit(1_700_000_000_u64 + 90 * 86_400)))
                    .order_by("starts_at", OrderDirection::Asc)
                    .offset(5)
                    .limit(10),
            )
            .expect("prepare calendar window");
        let map_viewport = db
            .prepare_query(
                &Query::from("venues")
                    .filter(gte(col("latitude"), lit(35.5_f64)))
                    .filter(lte(col("latitude"), lit(36.0_f64)))
                    .order_by("name", OrderDirection::Asc)
                    .limit(100),
            )
            .expect("prepare map viewport");
        Self {
            db,
            calendar_window,
            map_viewport,
        }
    }

    /// Ordered, bounded time-window query used by the itinerary calendar.
    pub fn calendar_window_count(&self) -> usize {
        self.db
            .read(&self.calendar_window)
            .expect("read calendar window")
            .len()
    }

    /// Latitude-bounded map viewport query.
    pub fn map_viewport_count(&self) -> usize {
        self.db
            .read(&self.map_viewport)
            .expect("read map viewport")
            .len()
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("tours").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("venues")
                    .column("name", ColumnType::Text)
                    .column("latitude", ColumnType::Double)
                    .column("longitude", ColumnType::Double)
                    .column("time_zone", ColumnType::Text)
                    .index_only(["latitude", "longitude", "name"]),
            )
            .table(
                TableSchemaBuilder::new("legs")
                    .fk_column("tour", "tours")
                    .fk_column("venue", "venues")
                    .column("starts_at", ColumnType::Timestamp)
                    .column("status", ColumnType::Text)
                    .index_only(["tour", "starts_at", "status"]),
            )
            .build(),
    )
    .expect("WorldTour benchmark schema compiles")
}

fn insert(db: &BenchDb, table: &str, id: RowUuid, cells: BTreeMap<String, Value>) {
    let write = block_on(db.insert_with_id(table, id, cells)).expect("insert fixture row");
    block_on(write.wait(DurabilityTier::Local)).expect("fixture row reaches local durability");
}

fn row_id(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}
