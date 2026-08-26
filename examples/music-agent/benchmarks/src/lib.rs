//! Self-contained MusicAgent large-value workloads.

use std::collections::BTreeMap;
use std::io::Cursor;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, PreparedQuery, block_on};
use jazz::groove::large_values::INLINE_VALUE_MAX_BYTES;
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

type BenchDb = Db<TestStorage>;

pub struct Fixture {
    db: BenchDb,
    storage: TestStorage,
    assistant: RowUuid,
    attachment: RowUuid,
    transcript: PreparedQuery,
    turns: TableSchema,
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

impl Fixture {
    pub fn new() -> Self {
        let schema = schema();
        let refs = schema.column_families();
        let storage = TestStorage::new(&refs.iter().map(String::as_str).collect::<Vec<_>>());
        let db = open(schema.clone(), storage.clone());
        let conversation = row_id(1);
        let assistant = row_id(2);
        let attachment = row_id(3);
        insert(
            &db,
            "conversations",
            conversation,
            BTreeMap::from([("title".into(), Value::String("Late listening".into()))]),
        );
        insert(
            &db,
            "turns",
            row_id(4),
            BTreeMap::from([
                ("conversation".into(), Value::Uuid(conversation.0)),
                ("ordinal".into(), Value::I32(0)),
                ("role".into(), Value::String("user".into())),
                ("body".into(), Value::String("warm saxophone".into())),
            ]),
        );
        let text = format!("{}final chorus", "a".repeat(INLINE_VALUE_MAX_BYTES * 2));
        let assistant_write = block_on(db.insert_streaming_value_with_id(
            "turns",
            assistant,
            BTreeMap::from([
                ("conversation".into(), Value::Uuid(conversation.0)),
                ("ordinal".into(), Value::I32(1)),
                ("role".into(), Value::String("assistant".into())),
            ]),
            "body",
            Cursor::new(text),
        ))
        .expect("stream assistant turn");
        block_on(assistant_write.wait(DurabilityTier::Local)).expect("durable assistant turn");
        let attachment_write = block_on(db.insert_streaming_value_with_id(
            "attachments",
            attachment,
            BTreeMap::from([("turn".into(), Value::Uuid(assistant.0))]),
            "payload",
            Cursor::new(vec![7_u8; INLINE_VALUE_MAX_BYTES * 2]),
        ))
        .expect("stream audio attachment");
        block_on(attachment_write.wait(DurabilityTier::Local)).expect("durable attachment");
        let transcript = db
            .prepare_query(
                &Query::from("turns")
                    .filter(eq(col("conversation"), lit(conversation.0)))
                    .order_by("ordinal", OrderDirection::Asc),
            )
            .expect("prepare transcript");
        Self {
            db,
            storage,
            assistant,
            attachment,
            transcript,
            turns: table(&schema, "turns"),
        }
    }

    pub fn append_assistant_tail(&self) {
        block_on(
            self.db
                .append_value("turns", self.assistant, "body", b"!".to_vec()),
        )
        .expect("append assistant tail");
    }

    pub fn attachment_range(&self) -> Vec<u8> {
        block_on(
            self.db
                .read_value_range("attachments", self.attachment, "payload", 64..128),
        )
        .expect("read attachment range")
    }

    pub fn materialized_transcript(&self) -> Vec<String> {
        self.db
            .read(&self.transcript)
            .expect("read transcript")
            .into_iter()
            .map(|row| match row.cell(&self.turns, "body") {
                Some(Value::String(body)) => body,
                other => panic!("unexpected transcript body: {other:?}"),
            })
            .collect()
    }

    pub fn restarted_transcript(&self) -> Vec<String> {
        let reopened = open(schema(), self.storage.clone());
        let conversation = row_id(1);
        let query = reopened
            .prepare_query(
                &Query::from("turns")
                    .filter(eq(col("conversation"), lit(conversation.0)))
                    .order_by("ordinal", OrderDirection::Asc),
            )
            .expect("prepare restarted transcript");
        let table = table(&schema(), "turns");
        reopened
            .read(&query)
            .expect("read restarted transcript")
            .into_iter()
            .map(|row| match row.cell(&table, "body") {
                Some(Value::String(body)) => body,
                other => panic!("unexpected restarted body: {other:?}"),
            })
            .collect()
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("conversations").column("title", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("turns")
                    .fk_column("conversation", "conversations")
                    .column("ordinal", ColumnType::Integer)
                    .column("role", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .index_only(["conversation", "ordinal"]),
            )
            .table(
                TableSchemaBuilder::new("attachments")
                    .fk_column("turn", "turns")
                    .column("payload", ColumnType::Bytea),
            )
            .build(),
    )
    .expect("MusicAgent schema compiles")
}

fn open(schema: JazzSchema, storage: TestStorage) -> BenchDb {
    block_on(Db::open(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x41; 16]),
            author: AuthorSubject::for_test_bytes([0x51; 16]),
        },
    )))
    .expect("open MusicAgent database")
}

fn table(schema: &JazzSchema, name: &str) -> TableSchema {
    schema
        .tables()
        .iter()
        .find(|table| table.name == name)
        .unwrap_or_else(|| panic!("MusicAgent schema has {name}"))
        .clone()
}

fn insert(db: &BenchDb, table: &str, row: RowUuid, cells: BTreeMap<String, Value>) {
    let write = block_on(db.insert(
        table,
        cells,
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .expect("insert MusicAgent fixture row");
    block_on(write.wait(DurabilityTier::Local)).expect("durable fixture row");
}

fn row_id(last: u8) -> RowUuid {
    RowUuid::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, last])
}
