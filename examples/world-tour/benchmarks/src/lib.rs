//! Self-contained WorldTour fixture and representative itinerary workloads.
//!
//! The benchmark intentionally duplicates the app schema subset required by
//! its query shapes; it does not depend on a frontend runtime or app fixture
//! helper.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, PreparedQuery, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, gte, lit, lte};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

type BenchDb = Db<MemoryStorage>;

const DAY_SECONDS: u64 = 86_400;
const WINDOW_START: u64 = 1_700_000_000;
const WINDOW_DAYS: u64 = 21;
const WINDOW_LIMIT: usize = 12;

/// A deterministic, in-memory WorldTour workload fixture.
pub struct Fixture {
    db: BenchDb,
    stops_table: TableSchema,
    venues_table: TableSchema,
    member_calendar_window: PreparedQuery,
    public_calendar_window: PreparedQuery,
}

impl Fixture {
    /// Seed `stop_count` dated itinerary stops across a predictable map grid.
    pub fn new(stop_count: usize) -> Self {
        assert!(stop_count >= 32, "fixture needs a useful itinerary window");
        Self::with_fixture(stop_count, stop_date, stop_status)
    }

    /// Seed a sparse fixture with one confirmed stop on each side of the
    /// inclusive three-week window and three confirmed stops inside it.
    pub fn boundary_receipt() -> Self {
        Self::with_fixture(5, boundary_stop_date, confirmed_status)
    }

    fn with_fixture(
        stop_count: usize,
        date_for_stop: fn(usize) -> u64,
        status_for_stop: fn(usize) -> &'static str,
    ) -> Self {
        let schema = schema();
        let stops_table = schema
            .tables()
            .iter()
            .find(|table| table.name == "stops")
            .expect("WorldTour benchmark schema has stops")
            .clone();
        let venues_table = schema
            .tables()
            .iter()
            .find(|table| table.name == "venues")
            .expect("WorldTour benchmark schema has venues")
            .clone();
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

        let band = row_id(1, 0);
        insert(
            &db,
            "bands",
            band,
            BTreeMap::from([("name".into(), Value::String("Summer world tour".into()))]),
        );
        // Insert in reverse chronological order so the workload receipt proves
        // that `order_by`, rather than insertion order, determines the result.
        for stop in (0..stop_count).rev() {
            let venue = row_id(2, stop);
            insert(
                &db,
                "venues",
                venue,
                BTreeMap::from([
                    ("name".into(), Value::String(format!("Venue {stop:05}"))),
                    ("city".into(), Value::String("Los Angeles".into())),
                    ("country".into(), Value::String("US".into())),
                    ("lat".into(), Value::F64(35.0 + (stop % 20) as f64 / 10.0)),
                    ("lng".into(), Value::F64(-120.0 + (stop % 30) as f64 / 10.0)),
                ]),
            );
            insert(
                &db,
                "stops",
                row_id(3, stop),
                BTreeMap::from([
                    ("bandId".into(), Value::Uuid(band.0)),
                    ("venueId".into(), Value::Uuid(venue.0)),
                    ("date".into(), Value::U64(date_for_stop(stop))),
                    ("status".into(), Value::String(status_for_stop(stop).into())),
                    (
                        "publicDescription".into(),
                        Value::String(format!("Stop {stop:05}")),
                    ),
                ]),
            );
        }

        let member_calendar_window = db
            .prepare_query(
                &Query::from("stops")
                    .filter(gte(col("date"), lit(WINDOW_START)))
                    .filter(lte(
                        col("date"),
                        lit(WINDOW_START + WINDOW_DAYS * DAY_SECONDS),
                    ))
                    .include("venueId")
                    .order_by("date", OrderDirection::Asc)
                    .limit(WINDOW_LIMIT),
            )
            .expect("prepare member calendar window");
        let public_calendar_window = db
            .prepare_query(
                &Query::from("stops")
                    .filter(eq(col("status"), lit("confirmed")))
                    .filter(gte(col("date"), lit(WINDOW_START)))
                    .filter(lte(
                        col("date"),
                        lit(WINDOW_START + WINDOW_DAYS * DAY_SECONDS),
                    ))
                    .include("venueId")
                    .order_by("date", OrderDirection::Asc)
                    .limit(WINDOW_LIMIT),
            )
            .expect("prepare public calendar window");
        Self {
            db,
            stops_table,
            venues_table,
            member_calendar_window,
            public_calendar_window,
        }
    }

    /// Ordered, bounded itinerary query used by band members.
    pub fn member_calendar_window_count(&self) -> usize {
        self.read_count(&self.member_calendar_window)
    }

    /// Ordered, bounded itinerary query used by public visitors.
    pub fn public_calendar_window_count(&self) -> usize {
        self.read_count(&self.public_calendar_window)
    }

    /// Ordered start times returned by the member itinerary query.
    pub fn member_calendar_window_start_times(&self) -> Vec<u64> {
        self.calendar_window_start_times(&self.member_calendar_window)
    }

    /// Ordered start times returned by the public, confirmed-only itinerary query.
    pub fn public_calendar_window_start_times(&self) -> Vec<u64> {
        self.calendar_window_start_times(&self.public_calendar_window)
    }

    /// Included venue names in the member root-row order.
    pub fn member_calendar_window_venue_names(&self) -> Vec<String> {
        self.calendar_window_venue_names(&self.member_calendar_window)
    }

    /// Included venue names in the public root-row order.
    pub fn public_calendar_window_venue_names(&self) -> Vec<String> {
        self.calendar_window_venue_names(&self.public_calendar_window)
    }

    fn read_count(&self, query: &PreparedQuery) -> usize {
        self.db.read(query).expect("read calendar window").len()
    }

    fn calendar_window_start_times(&self, query: &PreparedQuery) -> Vec<u64> {
        self.db
            .read(query)
            .expect("read calendar window")
            .into_iter()
            .map(|row| match row.cell(&self.stops_table, "date") {
                Some(Value::U64(value)) => value,
                other => panic!("unexpected date value: {other:?}"),
            })
            .collect()
    }

    fn calendar_window_venue_names(&self, query: &PreparedQuery) -> Vec<String> {
        assert_eq!(
            query
                .shape()
                .query()
                .includes
                .iter()
                .map(|include| include.path.as_str())
                .collect::<Vec<_>>(),
            ["venueId"],
            "calendar workload must retain the app's venue include",
        );
        self.db
            .read(query)
            .expect("read calendar window with included venues")
            .into_iter()
            .map(|stop| {
                let Some(Value::Uuid(venue_id)) = stop.cell(&self.stops_table, "venueId") else {
                    panic!("calendar stop has a venueId")
                };
                let venue_query = self
                    .db
                    .prepare_query(&Query::from("venues").filter(eq(col("id"), lit(venue_id))))
                    .expect("prepare included venue lookup");
                let venue = self
                    .db
                    .read(&venue_query)
                    .expect("read included venue")
                    .into_iter()
                    .next()
                    .expect("included venue is materialized");
                match venue.cell(&self.venues_table, "name") {
                    Some(Value::String(name)) => name,
                    other => panic!("unexpected included venue name: {other:?}"),
                }
            })
            .collect()
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("bands").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("venues")
                    .column("name", ColumnType::Text)
                    .column("city", ColumnType::Text)
                    .column("country", ColumnType::Text)
                    .column("lat", ColumnType::Double)
                    .column("lng", ColumnType::Double),
            )
            .table(
                TableSchemaBuilder::new("stops")
                    .fk_column("bandId", "bands")
                    .fk_column("venueId", "venues")
                    .column("date", ColumnType::Timestamp)
                    .column("status", ColumnType::Text)
                    .column("publicDescription", ColumnType::Text),
            )
            .build(),
    )
    .expect("WorldTour benchmark schema compiles")
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
    .expect("insert fixture row");
    block_on(write.wait(DurabilityTier::Local)).expect("fixture row reaches local durability");
}

fn row_id(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn stop_date(stop: usize) -> u64 {
    match stop {
        0 => WINDOW_START - DAY_SECONDS,
        // The app window contains more than the twelve returned rows, so a
        // removed `.limit(12)` is observable in the workload receipt.
        1..=22 => WINDOW_START + (stop as u64 - 1) * DAY_SECONDS,
        _ => WINDOW_START + (stop as u64 + 9) * DAY_SECONDS,
    }
}

fn stop_status(stop: usize) -> &'static str {
    // The public query must skip both non-confirmed states and still have more
    // than twelve candidates, making its predicate, order, and limit visible.
    if stop.is_multiple_of(5) {
        "tentative"
    } else if stop.is_multiple_of(7) {
        "cancelled"
    } else {
        "confirmed"
    }
}

fn boundary_stop_date(stop: usize) -> u64 {
    match stop {
        0 => WINDOW_START - DAY_SECONDS,
        1 => WINDOW_START,
        2 => WINDOW_START + DAY_SECONDS,
        3 => WINDOW_START + WINDOW_DAYS * DAY_SECONDS,
        4 => WINDOW_START + (WINDOW_DAYS + 1) * DAY_SECONDS,
        _ => unreachable!("boundary fixture has exactly five stops"),
    }
}

fn confirmed_status(_stop: usize) -> &'static str {
    "confirmed"
}
