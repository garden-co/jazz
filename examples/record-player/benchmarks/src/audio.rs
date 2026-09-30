//! Scrubbing a track: reading one 64 KiB window from the middle of a large
//! streamed audio value, as the player does when a listener drags the
//! playhead.
//!
//! Moved from EpicDrop's `epic_drop_seek_64mb` (which also covered MusicAgent's
//! `music_agent_attachment_seek_8mb`), reframed in RecordPlayer terms: the
//! folder is an album and the file is a track's audio. Storage, value size,
//! streaming source and the timed range read are unchanged.

use std::collections::BTreeMap;
use std::io::Read;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, block_on};
use jazz::groove::large_values::INLINE_VALUE_MAX_BYTES;
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::schema::JazzSchema;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

const SOURCE_READ_BYTES: usize = 32 * 1024;
/// The window the player reads per scrub.
pub const SCRUB_BYTES: u64 = 64 * 1024;

fn album_id() -> RowUuid {
    RowUuid::from_bytes([0x41; 16])
}

fn track_id() -> RowUuid {
    RowUuid::from_bytes([0x42; 16])
}

/// A deterministic source that never returns more than one bounded source
/// window, even when a caller offers a much larger buffer.
struct PatternReader {
    position: usize,
    length: usize,
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

/// One album holding one track whose audio of `audio_bytes` was streamed in.
pub struct ScrubFixture {
    db: Db,
    audio_bytes: usize,
}

impl ScrubFixture {
    pub fn new(audio_bytes: usize) -> Self {
        assert!(
            audio_bytes > INLINE_VALUE_MAX_BYTES,
            "exercise the indirect large-value path"
        );
        let schema = schema();
        let refs = schema.column_families();
        let storage = TestStorage::new(&refs.iter().map(String::as_str).collect::<Vec<_>>());
        let db = block_on(Db::open(DbConfig::new(
            schema,
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([0xe1; 16]),
                author: AuthorSubject::for_test_bytes([0xe2; 16]),
            },
        )))
        .expect("open RecordPlayer audio database");
        insert_album(&db);
        stream_audio(&db, audio_bytes);
        Self { db, audio_bytes }
    }

    /// The timed operation: read the 64 KiB window at the track's midpoint.
    pub fn scrub_to_middle(&self) -> Vec<u8> {
        let start = (self.audio_bytes / 2) as u64;
        block_on(self.db.read_value_range(
            "tracks",
            track_id(),
            "audio",
            start..start + SCRUB_BYTES,
        ))
        .expect("read audio window")
    }
}

/// The deterministic source bytes the midpoint window must return.
pub fn expected_scrub_window(audio_bytes: usize) -> Vec<u8> {
    let start = audio_bytes / 2;
    (start..start + SCRUB_BYTES as usize)
        .map(|offset| (offset % 251) as u8)
        .collect()
}

fn stream_audio(db: &Db, audio_bytes: usize) {
    let write = block_on(db.insert_streaming_value_with_id(
        "tracks",
        track_id(),
        BTreeMap::from([
            ("album_id".to_owned(), Value::Uuid(album_id().0)),
            ("title".to_owned(), Value::String("live-set.wav".to_owned())),
            (
                "content_type".to_owned(),
                Value::String("audio/wav".to_owned()),
            ),
            (
                "size_bytes".to_owned(),
                Value::I32(i32::try_from(audio_bytes).expect("benchmark audio size fits an int")),
            ),
            (
                "owner_id".to_owned(),
                Value::String("demo-owner".to_owned()),
            ),
        ]),
        "audio",
        PatternReader {
            position: 0,
            length: audio_bytes,
        },
    ))
    .expect("stream audio into fixture");
    block_on(write.wait(DurabilityTier::Local)).expect("streamed audio reaches local durability");
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("albums")
                    .column("name", ColumnType::Text)
                    .column("owner_id", ColumnType::Text),
            )
            .table(
                TableSchemaBuilder::new("tracks")
                    .fk_column("album_id", "albums")
                    .column("title", ColumnType::Text)
                    .column("content_type", ColumnType::Text)
                    .column("size_bytes", ColumnType::Integer)
                    .column("owner_id", ColumnType::Text)
                    .column("audio", ColumnType::Bytea)
                    .index_only(["album_id"]),
            )
            .build()
            .allow_all(),
    )
    .expect("RecordPlayer audio schema compiles")
}

fn insert_album(db: &Db) {
    let write = block_on(db.insert(
        "albums",
        BTreeMap::from([
            ("name".to_owned(), Value::String("Demos".to_owned())),
            (
                "owner_id".to_owned(),
                Value::String("demo-owner".to_owned()),
            ),
        ]),
        InsertOptions {
            row_id: Some(album_id()),
            ..Default::default()
        },
    ))
    .expect("insert benchmark album");
    block_on(write.wait(DurabilityTier::Local)).expect("album reaches local durability");
}
