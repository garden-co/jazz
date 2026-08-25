//! Self-contained EpicDrop fixture for streamed file creation, metadata
//! listing, and bounded range downloads. The benchmark deliberately does not
//! import the React app: its schema and synthetic data stay inspectable here.

use std::collections::BTreeMap;
use std::io::Read;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, block_on};
use jazz::groove::large_values::LargeValueKind;
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

const CHUNK_BYTES: usize = 32 * 1024;
type BenchDb = Db<MemoryStorage>;

fn folder_id() -> RowUuid {
    RowUuid::from_bytes([0x41; 16])
}

fn file_id() -> RowUuid {
    RowUuid::from_bytes([0x42; 16])
}

/// A deterministic reader that produces content without retaining a complete
/// file-sized allocation. Byte `n` is always `n % 251`.
struct PatternReader {
    position: usize,
    length: usize,
}

impl PatternReader {
    fn new(length: usize) -> Self {
        Self {
            position: 0,
            length,
        }
    }
}

impl Read for PatternReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.length.saturating_sub(self.position);
        let count = remaining.min(buffer.len()).min(CHUNK_BYTES);
        for (offset, byte) in buffer[..count].iter_mut().enumerate() {
            *byte = ((self.position + offset) % 251) as u8;
        }
        self.position += count;
        Ok(count)
    }
}

pub struct Fixture {
    db: BenchDb,
    list: jazz::db::PreparedQuery,
    file_bytes: usize,
}

impl Fixture {
    pub fn new(file_bytes: usize) -> Self {
        assert!(
            file_bytes > CHUNK_BYTES,
            "exercise the indirect large-value path"
        );
        let (db, table) = open_db();
        insert_folder(&db);
        let write = block_on(db.insert_streaming_value_with_id(
            "files",
            file_id(),
            BTreeMap::from([
                ("folder_id".to_owned(), Value::Uuid(folder_id().0)),
                ("name".to_owned(), Value::String("live-set.wav".to_owned())),
                (
                    "content_type".to_owned(),
                    Value::String("audio/wav".to_owned()),
                ),
                (
                    "size_bytes".to_owned(),
                    Value::I32(i32::try_from(file_bytes).expect("benchmark file size fits an int")),
                ),
                (
                    "owner_id".to_owned(),
                    Value::String("demo-owner".to_owned()),
                ),
            ]),
            "contents",
            LargeValueKind::Bytes,
            PatternReader::new(file_bytes),
        ))
        .expect("stream file into fixture");
        block_on(write.wait(DurabilityTier::Local)).expect("fixture file reaches local durability");
        let list = db
            .prepare_query(
                &Query::from("files")
                    .filter(eq(col("folder_id"), lit(folder_id().0)))
                    .select(["id", "name", "content_type", "size_bytes"])
                    .order_by("name", OrderDirection::Asc),
            )
            .expect("prepare EpicDrop folder listing");
        debug_assert_eq!(table.name, "files");
        Self {
            db,
            list,
            file_bytes,
        }
    }

    pub fn list_folder(&self) -> usize {
        self.db.read(&self.list).expect("list fixture folder").len()
    }

    pub fn download_middle_range(&self) -> Vec<u8> {
        let start = (self.file_bytes / 2) as u64;
        block_on(
            self.db
                .read_value_range("files", file_id(), "contents", start..start + 64 * 1024),
        )
        .expect("read bounded file range")
    }
}

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("folders")
                .column("name", ColumnType::Text)
                .column("owner_id", ColumnType::Text),
        )
        .table(
            TableSchemaBuilder::new("files")
                .fk_column("folder_id", "folders")
                .column("name", ColumnType::Text)
                .column("content_type", ColumnType::Text)
                .column("size_bytes", ColumnType::Integer)
                .column("owner_id", ColumnType::Text)
                .column("contents", ColumnType::Bytea)
                .index_only(["folder_id"]),
        )
        .build();
    JazzSchema::new(&source).expect("EpicDrop benchmark schema compiles")
}

fn open_db() -> (BenchDb, TableSchema) {
    let schema = schema();
    let table = schema
        .tables()
        .iter()
        .find(|table| table.name == "files")
        .expect("files table")
        .clone();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        MemoryStorage::new(&family_refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0xe1; 16]),
            // Native fixtures use an explicit canonical system subject rather
            // than the retired raw author-id alias.
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open EpicDrop benchmark database");
    (db, table)
}

fn insert_folder(db: &BenchDb) {
    let write = block_on(db.insert(
        "folders",
        BTreeMap::from([
            ("name".to_owned(), Value::String("Demos".to_owned())),
            (
                "owner_id".to_owned(),
                Value::String("demo-owner".to_owned()),
            ),
        ]),
        InsertOptions {
            row_id: Some(folder_id()),
            ..Default::default()
        },
    ))
    .expect("insert benchmark folder");
    block_on(write.wait(DurabilityTier::Local)).expect("folder reaches local durability");
}

pub fn expected_range(file_bytes: usize) -> Vec<u8> {
    let start = file_bytes / 2;
    (start..start + 64 * 1024)
        .map(|offset| (offset % 251) as u8)
        .collect()
}
