//! Self-contained BandChat schema, fixture, and representative read workloads.
//!
//! The benchmark intentionally duplicates this small schema surface rather
//! than importing application runtime or fixture helpers.

mod fast_resume;

pub use fast_resume::{FastResumeFixture, FastResumeReceipt};

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, PreparedQuery, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

const AUTHORS: usize = 32;
const MESSAGES_PER_ROOM: usize = 16;
const HOT_ROOM_MESSAGES: usize = 100;
const TIMELINE_OFFSET: usize = 25;
const TIMELINE_PAGE: usize = 25;

type BenchDb = Db<MemoryStorage>;

pub struct Fixture {
    db: BenchDb,
    messages_table: TableSchema,
    memberships_table: TableSchema,
    timeline: PreparedQuery,
    unread_rooms: PreparedQuery,
    author_history: PreparedQuery,
}

impl Fixture {
    /// Open, seed, and prepare all query shapes before benchmark timing starts.
    pub fn new(message_count: usize) -> Self {
        assert!(message_count >= HOT_ROOM_MESSAGES + MESSAGES_PER_ROOM);
        assert!(message_count.is_multiple_of(MESSAGES_PER_ROOM));
        let room_count = message_count / MESSAGES_PER_ROOM;
        let (db, messages_table, memberships_table) = open_db();

        for author in 0..AUTHORS {
            insert(
                &db,
                "users",
                row_id(1, author),
                BTreeMap::from([(
                    "display_name".to_owned(),
                    Value::String(format!("Member {author:02}")),
                )]),
            );
        }
        for room in 0..room_count {
            insert(
                &db,
                "rooms",
                row_id(2, room),
                BTreeMap::from([("name".to_owned(), Value::String(format!("Room {room:04}")))]),
            );
            insert(
                &db,
                "memberships",
                row_id(3, room),
                BTreeMap::from([
                    ("room".to_owned(), Value::Uuid(row_id(2, room).0)),
                    (
                        "member".to_owned(),
                        Value::Uuid(row_id(1, room % AUTHORS).0),
                    ),
                    (
                        "unread".to_owned(),
                        Value::Bool((room / AUTHORS).is_multiple_of(2)),
                    ),
                    ("last_activity".to_owned(), Value::U64(room as u64)),
                ]),
            );
        }
        for message in 0..message_count {
            let room = if message < HOT_ROOM_MESSAGES {
                0
            } else {
                1 + (message - HOT_ROOM_MESSAGES) % (room_count - 1)
            };
            insert(
                &db,
                "messages",
                row_id(4, message),
                BTreeMap::from([
                    ("room".to_owned(), Value::Uuid(row_id(2, room).0)),
                    (
                        "author".to_owned(),
                        Value::Uuid(row_id(1, message % AUTHORS).0),
                    ),
                    (
                        "body".to_owned(),
                        Value::String(format!("Message {message:06}")),
                    ),
                    ("sent_at".to_owned(), Value::U64(message as u64)),
                ]),
            );
        }

        let timeline = db
            .prepare_query(
                &Query::from("messages")
                    .filter(eq(col("room"), lit(row_id(2, 0).0)))
                    .order_by("sent_at", OrderDirection::Desc)
                    .offset(TIMELINE_OFFSET)
                    .limit(TIMELINE_PAGE),
            )
            .expect("prepare room timeline page");
        let unread_rooms = db
            .prepare_query(
                &Query::from("memberships")
                    .filter(eq(col("member"), lit(row_id(1, 0).0)))
                    .filter(eq(col("unread"), lit(true)))
                    .order_by("last_activity", OrderDirection::Desc),
            )
            .expect("prepare unread recent-room lookup");
        let author_history = db
            .prepare_query(
                &Query::from("messages")
                    .filter(eq(col("author"), lit(row_id(1, 0).0)))
                    .order_by("sent_at", OrderDirection::Desc),
            )
            .expect("prepare author message history");

        Self {
            db,
            messages_table,
            memberships_table,
            timeline,
            unread_rooms,
            author_history,
        }
    }

    pub fn timeline_page_count(&self) -> usize {
        self.read_count(&self.timeline)
    }

    pub fn unread_room_count(&self) -> usize {
        self.read_count(&self.unread_rooms)
    }

    pub fn author_history_count(&self) -> usize {
        self.read_count(&self.author_history)
    }

    pub fn timeline_sent_at(&self) -> Vec<u64> {
        self.ordered_values(&self.timeline, &self.messages_table, "sent_at")
    }

    pub fn unread_room_activity(&self) -> Vec<u64> {
        self.ordered_values(&self.unread_rooms, &self.memberships_table, "last_activity")
    }

    pub fn author_history_sent_at(&self) -> Vec<u64> {
        self.ordered_values(&self.author_history, &self.messages_table, "sent_at")
    }

    fn read_count(&self, query: &PreparedQuery) -> usize {
        self.db
            .read(query)
            .expect("BandChat benchmark read succeeds")
            .len()
    }

    fn ordered_values(&self, query: &PreparedQuery, table: &TableSchema, column: &str) -> Vec<u64> {
        self.db
            .read(query)
            .expect("BandChat benchmark read succeeds")
            .into_iter()
            .map(|row| match row.cell(table, column) {
                Some(Value::U64(value)) => value,
                other => panic!("unexpected {column} value: {other:?}"),
            })
            .collect()
    }
}

pub fn expected_counts(message_count: usize) -> (usize, usize, usize) {
    let room_count = message_count / MESSAGES_PER_ROOM;
    (
        TIMELINE_PAGE,
        room_count.div_ceil(AUTHORS * 2),
        message_count.div_ceil(AUTHORS),
    )
}

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("users").column("display_name", ColumnType::Text))
        .table(TableSchemaBuilder::new("rooms").column("name", ColumnType::Text))
        .table(
            TableSchemaBuilder::new("memberships")
                .fk_column("room", "rooms")
                .fk_column("member", "users")
                .column("unread", ColumnType::Boolean)
                .column("last_activity", ColumnType::Timestamp)
                .index_only(["room", "member", "unread", "last_activity"]),
        )
        .table(
            TableSchemaBuilder::new("messages")
                .fk_column("room", "rooms")
                .fk_column("author", "users")
                .column("body", ColumnType::Text)
                .column("sent_at", ColumnType::Timestamp)
                .index_only(["room", "author", "sent_at"]),
        )
        .build();
    JazzSchema::new(&source).expect("BandChat benchmark schema compiles")
}

fn open_db() -> (BenchDb, TableSchema, TableSchema) {
    let schema = schema();
    let table = |name: &str| {
        schema
            .tables()
            .iter()
            .find(|table| table.name == name)
            .unwrap_or_else(|| panic!("BandChat schema has {name}"))
            .clone()
    };
    let messages_table = table("messages");
    let memberships_table = table("memberships");
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        MemoryStorage::new(&family_refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0xbc; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open BandChat benchmark database");
    (db, messages_table, memberships_table)
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
    .expect("insert BandChat fixture row");
    block_on(write.wait(DurabilityTier::Local)).expect("fixture row reaches local durability");
}

fn row_id(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}
