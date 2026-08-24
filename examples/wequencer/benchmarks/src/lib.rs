//! Self-contained Wequencer fixture and sequencer-shaped workloads.
//!
//! The native model deliberately duplicates the application schema and query
//! shapes. It does not import a shared application helper.

use std::collections::BTreeMap;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, PreparedQuery, ReadOpts, SubscriptionEvent,
    UpdateOptions, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

pub const TRACKS: usize = 16;
pub const STEPS: usize = 64;

type BenchDb = Db<MemoryStorage>;

pub struct Fixture {
    db: BenchDb,
    step_table: TableSchema,
    track_window: PreparedQuery,
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

impl Fixture {
    pub fn new() -> Self {
        let (db, step_table) = open_db();
        let pattern_id = row_id(1, 0);
        for track in 0..TRACKS {
            insert(
                &db,
                "tracks",
                row_id(2, track),
                BTreeMap::from([
                    ("pattern_id".into(), Value::Uuid(pattern_id.0)),
                    ("position".into(), Value::I32(track as i32)),
                    ("name".into(), Value::String(format!("Track {track:02}"))),
                ]),
            );
            for step in 0..STEPS {
                insert(
                    &db,
                    "steps",
                    row_id(3 + track as u8, step),
                    BTreeMap::from([
                        ("track_id".into(), Value::Uuid(row_id(2, track).0)),
                        ("position".into(), Value::I32(step as i32)),
                        ("velocity".into(), Value::I32(((track + step) % 128) as i32)),
                        ("enabled".into(), Value::Bool((track + step) % 3 == 0)),
                    ]),
                );
            }
        }
        let track_window = db
            .prepare_query(
                &Query::from("steps")
                    .filter(eq(col("track_id"), lit(row_id(2, 0).0)))
                    .order_by("position", OrderDirection::Asc),
            )
            .expect("prepare Wequencer track query");
        Self {
            db,
            step_table,
            track_window,
        }
    }

    pub fn track_steps(&self) -> Vec<(u64, bool)> {
        self.db
            .read(&self.track_window)
            .expect("read Wequencer track")
            .into_iter()
            .map(|row| {
                let position = match row.cell(&self.step_table, "position") {
                    Some(Value::I32(position)) => position as u64,
                    value => panic!("unexpected step position: {value:?}"),
                };
                let enabled = match row.cell(&self.step_table, "enabled") {
                    Some(Value::Bool(enabled)) => enabled,
                    value => panic!("unexpected enabled value: {value:?}"),
                };
                (position, enabled)
            })
            .collect()
    }

    pub fn playhead_window(&self, from: usize, length: usize) -> Vec<(u64, bool)> {
        self.track_steps()
            .into_iter()
            .skip(from)
            .take(length)
            .collect()
    }

    pub fn concurrent_edit_burst(&self, editor_count: usize) -> usize {
        assert!(editor_count > 0);
        for editor in 0..editor_count {
            let step = editor % STEPS;
            let write = block_on(self.db.update(
                "steps",
                row_id(3, step),
                BTreeMap::from([
                    ("enabled".into(), Value::Bool(editor % 2 == 0)),
                    ("velocity".into(), Value::I32((96 + editor % 32) as i32)),
                ]),
                UpdateOptions::default(),
            ))
            .expect("edit Wequencer step");
            block_on(write.wait(DurabilityTier::Local)).expect("step reaches local durability");
        }
        self.track_steps()
            .iter()
            .filter(|(_, enabled)| *enabled)
            .count()
    }

    /// Opens the same ordered query shape the UI subscribes to, drains its
    /// initial result, and observes one edited pad through the public stream.
    pub fn subscribed_step_edit(&self) -> bool {
        let mut subscription = block_on(self.db.subscribe(&self.track_window, ReadOpts::default()))
            .expect("open Wequencer step subscription");
        let initial =
            block_on(subscription.next_event()).expect("subscription has initial step snapshot");
        assert!(matches!(
            initial,
            SubscriptionEvent::Delta { reset: true, .. }
        ));

        let write = block_on(self.db.update(
            "steps",
            row_id(3, 0),
            BTreeMap::from([("enabled".into(), Value::Bool(false))]),
            UpdateOptions::default(),
        ))
        .expect("edit subscribed Wequencer step");
        block_on(write.wait(DurabilityTier::Local))
            .expect("subscribed step reaches local durability");
        matches!(
            block_on(subscription.next_event()).expect("subscription observes local step edit"),
            SubscriptionEvent::Delta { updated, .. } if !updated.is_empty()
        )
    }

    /// Models the subscription fan-out behind a shared pattern-grid hotspot.
    /// Each listener is a separate public subscription over the same ordered
    /// track. The benchmark measures initial materialization plus delivery of
    /// an edited pad to every listener.
    pub fn subscribed_step_fanout(&self, subscribers: usize) -> usize {
        assert!(subscribers > 0, "fan-out requires at least one subscriber");
        let mut streams = (0..subscribers)
            .map(|_| {
                let mut stream =
                    block_on(self.db.subscribe(&self.track_window, ReadOpts::default()))
                        .expect("open Wequencer step subscription");
                let initial =
                    block_on(stream.next_event()).expect("subscription has initial step snapshot");
                assert!(matches!(
                    initial,
                    SubscriptionEvent::Delta { reset: true, .. }
                ));
                stream
            })
            .collect::<Vec<_>>();

        let write = block_on(self.db.update(
            "steps",
            row_id(3, 0),
            BTreeMap::from([("enabled".into(), Value::Bool(false))]),
        ))
        .expect("edit fan-out Wequencer step");
        block_on(write.wait(DurabilityTier::Local)).expect("fan-out step reaches local durability");

        streams
            .iter_mut()
            .map(|stream| {
                matches!(
                    block_on(stream.next_event()).expect("subscription observes fan-out step edit"),
                    SubscriptionEvent::Delta { updated, .. } if !updated.is_empty()
                )
            })
            .filter(|delivered| *delivered)
            .count()
    }
}

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("patterns")
                .column("title", ColumnType::Text)
                .column("tempo", ColumnType::Integer),
        )
        .table(
            TableSchemaBuilder::new("tracks")
                .fk_column("pattern_id", "patterns")
                .column("position", ColumnType::Integer)
                .column("name", ColumnType::Text)
                .index_only(["pattern_id", "position"]),
        )
        .table(
            TableSchemaBuilder::new("steps")
                .fk_column("track_id", "tracks")
                .column("position", ColumnType::Integer)
                .column("velocity", ColumnType::Integer)
                .column("enabled", ColumnType::Boolean)
                .index_only(["track_id", "position"]),
        )
        .build();
    JazzSchema::new(&source).expect("Wequencer schema compiles")
}

fn open_db() -> (BenchDb, TableSchema) {
    let schema = schema();
    let step_table = schema
        .tables()
        .iter()
        .find(|table| table.name == "steps")
        .expect("steps table")
        .clone();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        MemoryStorage::new(&family_refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0xe1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open Wequencer benchmark database");
    insert(
        &db,
        "patterns",
        row_id(1, 0),
        BTreeMap::from([
            ("title".into(), Value::String("Weekend set".into())),
            ("tempo".into(), Value::I32(124)),
        ]),
    );
    (db, step_table)
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
    .expect("insert Wequencer row");
    block_on(write.wait(DurabilityTier::Local)).expect("fixture row reaches local durability");
}

fn row_id(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}
