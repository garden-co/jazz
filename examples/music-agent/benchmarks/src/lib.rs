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
use jazz::tools::test_support::AllowAll;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

type BenchDb = Db<TestStorage>;

const SEEK_BYTES: u64 = 64 * 1024;
const SHORT_REPLY: &str = "try a late-night quartet with brushed drums, upright bass and a tenor \
saxophone carrying the melody; start with a ballad, then something with a slow swing.";

pub struct Fixture {
    db: BenchDb,
    storage: TestStorage,
    assistant: RowUuid,
    attachment: RowUuid,
    attachment_bytes: usize,
    transcript: PreparedQuery,
    turns: TableSchema,
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

impl Fixture {
    /// One user prompt followed by one streamed assistant turn, plus a small
    /// audio attachment on that turn.
    pub fn new() -> Self {
        Self::with_shape(2, INLINE_VALUE_MAX_BYTES * 2)
    }

    /// A conversation of `turn_count` turns. Earlier turns alternate short
    /// user and assistant messages; the final turn is a long assistant reply
    /// streamed in as a large value, carrying an audio attachment of
    /// `attachment_bytes`.
    pub fn with_shape(turn_count: usize, attachment_bytes: usize) -> Self {
        assert!(turn_count >= 2, "fixture requires a prompt and a reply");
        assert!(
            attachment_bytes > INLINE_VALUE_MAX_BYTES,
            "exercise the indirect large-value path"
        );
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
        for ordinal in 1..turn_count - 1 {
            let (role, body) = if ordinal % 2 == 1 {
                (
                    "assistant",
                    format!("Suggestion {ordinal}: {}", SHORT_REPLY),
                )
            } else {
                ("user", format!("Follow-up {ordinal}: something slower?"))
            };
            insert(
                &db,
                "turns",
                history_turn_id(ordinal),
                BTreeMap::from([
                    ("conversation".into(), Value::Uuid(conversation.0)),
                    ("ordinal".into(), Value::I32(ordinal as i32)),
                    ("role".into(), Value::String(role.into())),
                    ("body".into(), Value::String(body)),
                ]),
            );
        }
        let text = format!("{}final chorus", "a".repeat(INLINE_VALUE_MAX_BYTES * 2));
        let assistant_write = block_on(db.insert_streaming_value_with_id(
            "turns",
            assistant,
            BTreeMap::from([
                ("conversation".into(), Value::Uuid(conversation.0)),
                ("ordinal".into(), Value::I32(turn_count as i32 - 1)),
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
            Cursor::new(attachment_pattern(0..attachment_bytes)),
        ))
        .expect("stream audio attachment");
        block_on(attachment_write.wait(DurabilityTier::Local)).expect("durable attachment");
        let transcript = db
            .prepare_query(&transcript_query())
            .expect("prepare transcript");
        Self {
            db,
            storage,
            assistant,
            attachment,
            attachment_bytes,
            transcript,
            turns: table(&schema, "turns"),
        }
    }

    /// Stream `chunk_count` chunks of `chunk_bytes` onto the assistant turn,
    /// one append per chunk as a token stream arrives, and wait until the
    /// last one is locally durable.
    pub fn stream_reply(&self, chunk_count: usize, chunk_bytes: usize) -> usize {
        let chunk = "x".repeat(chunk_bytes).into_bytes();
        let mut last = None;
        for _ in 0..chunk_count {
            last = Some(
                block_on(
                    self.db
                        .append_value("turns", self.assistant, "body", chunk.clone()),
                )
                .expect("append streamed chunk"),
            );
        }
        if let Some(write) = last {
            block_on(write.wait(DurabilityTier::Local)).expect("streamed reply is durable");
        }
        chunk_count
    }

    /// A 64 KiB window from the middle of the audio attachment, as a player
    /// requests when the user seeks.
    pub fn attachment_seek(&self) -> Vec<u8> {
        let start = (self.attachment_bytes / 2) as u64;
        block_on(self.db.read_value_range(
            "attachments",
            self.attachment,
            "payload",
            start..start + SEEK_BYTES,
        ))
        .expect("read attachment window")
    }

    pub fn expected_attachment_seek(&self) -> Vec<u8> {
        let start = self.attachment_bytes / 2;
        attachment_pattern(start..start + SEEK_BYTES as usize)
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
        let query = reopened
            .prepare_query(&transcript_query())
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

fn transcript_query() -> Query {
    Query::from("turns")
        .filter(eq(col("conversation"), lit(row_id(1).0)))
        .order_by("ordinal", OrderDirection::Asc)
}

fn attachment_pattern(range: std::ops::Range<usize>) -> Vec<u8> {
    range.map(|offset| (offset % 251) as u8).collect()
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
            .build()
            .allow_all(),
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

fn history_turn_id(ordinal: usize) -> RowUuid {
    let mut bytes = [0x5a; 16];
    bytes[8..].copy_from_slice(&(ordinal as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn row_id(last: u8) -> RowUuid {
    RowUuid::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, last])
}
