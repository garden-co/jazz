//! EpicDrop's native large-value workload is intentionally narrow: write one
//! file from a bounded reader, list its metadata, then read one byte window.

use std::collections::BTreeMap;
use std::io::Read;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, PreparedQuery, block_on};
use jazz::groove::large_values::INLINE_VALUE_MAX_BYTES;
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

const SOURCE_READ_BYTES: usize = 32 * 1024;
const RANGE_BYTES: u64 = 64 * 1024;
type BenchDb = Db<TestStorage>;

fn folder_id() -> RowUuid {
    RowUuid::from_bytes([0x41; 16])
}

fn file_id() -> RowUuid {
    RowUuid::from_bytes([0x42; 16])
}

/// A deterministic source that never returns more than one bounded source
/// window, even when a caller offers a much larger buffer.
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
        let count = remaining.min(buffer.len()).min(SOURCE_READ_BYTES);
        for (offset, byte) in buffer[..count].iter_mut().enumerate() {
            *byte = ((self.position + offset) % 251) as u8;
        }
        self.position += count;
        Ok(count)
    }
}

pub struct Fixture {
    db: BenchDb,
    file_bytes: usize,
    list: PreparedQuery,
}

impl Fixture {
    /// One folder holding one streamed file of `file_bytes`.
    pub fn new(file_bytes: usize) -> Self {
        Self::with_files(1, file_bytes)
    }

    /// One folder holding `file_count` streamed files of `file_bytes` each.
    /// The first file is the range/download target.
    pub fn with_files(file_count: usize, file_bytes: usize) -> Self {
        assert!(file_count > 0, "fixture requires at least one file");
        assert!(
            file_bytes > INLINE_VALUE_MAX_BYTES,
            "exercise the indirect large-value path"
        );
        let db = open_seeded_db();
        stream_file(&db, file_id(), "live-set.wav", file_bytes);
        for index in 1..file_count {
            stream_file(
                &db,
                numbered_file_id(index),
                &format!("take-{index:04}.wav"),
                file_bytes,
            );
        }

        let list = db
            .prepare_query(
                &Query::from("files")
                    .filter(eq(col("folder_id"), lit(folder_id().0)))
                    .select(["id", "name", "content_type", "size_bytes"])
                    .order_by("name", OrderDirection::Asc),
            )
            .expect("prepare EpicDrop metadata listing");
        Self {
            db,
            file_bytes,
            list,
        }
    }

    pub fn list_folder(&self) -> usize {
        self.db.read(&self.list).expect("list fixture folder").len()
    }

    pub fn download_middle_range(&self) -> Vec<u8> {
        let start = (self.file_bytes / 2) as u64;
        block_on(self.db.read_value_range(
            "files",
            file_id(),
            "contents",
            start..start + RANGE_BYTES,
        ))
        .expect("read bounded file range")
    }

    /// Read the whole first file back, as a "download" button would.
    pub fn download_file(&self) -> Vec<u8> {
        block_on(self.db.read_value_range(
            "files",
            file_id(),
            "contents",
            0..self.file_bytes as u64,
        ))
        .expect("read whole file")
    }
}

/// An opened EpicDrop database with its folder in place and no files yet.
/// Each upload streams one new file and waits for local durability.
pub struct UploadFixture {
    db: BenchDb,
}

impl UploadFixture {
    pub fn new() -> Self {
        Self {
            db: open_seeded_db(),
        }
    }

    pub fn upload(&self, index: usize, file_bytes: usize) -> usize {
        stream_file(
            &self.db,
            numbered_file_id(index),
            &format!("upload-{index:04}.wav"),
            file_bytes,
        );
        file_bytes
    }

    /// Untimed receipt: the uploaded file's listed size and a byte window.
    pub fn uploaded_range(&self, index: usize, range: std::ops::Range<u64>) -> Vec<u8> {
        block_on(
            self.db
                .read_value_range("files", numbered_file_id(index), "contents", range),
        )
        .expect("read uploaded file range")
    }
}

impl Default for UploadFixture {
    fn default() -> Self {
        Self::new()
    }
}

fn numbered_file_id(index: usize) -> RowUuid {
    let mut bytes = [0x43; 16];
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn open_seeded_db() -> BenchDb {
    let schema = schema();
    let refs = schema.column_families();
    let storage = TestStorage::new(&refs.iter().map(String::as_str).collect::<Vec<_>>());
    let db = open(schema, storage);
    insert_folder(&db);
    db
}

fn stream_file(db: &BenchDb, id: RowUuid, name: &str, file_bytes: usize) {
    let write = block_on(db.insert_streaming_value_with_id(
        "files",
        id,
        BTreeMap::from([
            ("folder_id".to_owned(), Value::Uuid(folder_id().0)),
            ("name".to_owned(), Value::String(name.to_owned())),
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
        PatternReader::new(file_bytes),
    ))
    .expect("stream file into fixture");
    block_on(write.wait(DurabilityTier::Local)).expect("streamed file reaches local durability");
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
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
            .build()
            .allow_all(),
    )
    .expect("EpicDrop benchmark schema compiles")
}

fn open(schema: JazzSchema, storage: TestStorage) -> BenchDb {
    block_on(Db::open(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0xe1; 16]),
            author: AuthorSubject::for_test_bytes([0xe2; 16]),
        },
    )))
    .expect("open EpicDrop benchmark database")
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
    expected_bytes(start..start + RANGE_BYTES as usize)
}

/// The deterministic source pattern for any byte window of a fixture file.
pub fn expected_bytes(range: std::ops::Range<usize>) -> Vec<u8> {
    range.map(|offset| (offset % 251) as u8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // This internal receipt protects the benchmark source's bounded-memory
    // premise; the bound is not otherwise observable through its timing API.
    #[test]
    fn pattern_reader_caps_each_source_read() {
        let mut reader = PatternReader::new(SOURCE_READ_BYTES * 3);
        let mut oversized_buffer = vec![0; SOURCE_READ_BYTES * 4];
        assert_eq!(
            reader.read(&mut oversized_buffer).unwrap(),
            SOURCE_READ_BYTES
        );
        assert_eq!(
            reader.read(&mut oversized_buffer).unwrap(),
            SOURCE_READ_BYTES
        );
    }
}
