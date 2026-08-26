//! Self-contained RecordPlayer metadata and playlist-window workloads.

use jazz::db::{Db, DbConfig, DbIdentity, PreparedQuery, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::collections::BTreeMap;

type BenchDb = Db<MemoryStorage>;

/// Deterministic record catalogue and one ordered playlist.
pub struct Fixture {
    db: BenchDb,
    coverflow: PreparedQuery,
    playlist_window: PreparedQuery,
}

impl Fixture {
    pub fn new(track_count: usize) -> Self {
        assert!(track_count >= 32, "fixture needs a useful playlist window");
        let schema = schema();
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let db = block_on(Db::open(DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x52; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .expect("open RecordPlayer benchmark database");
        let playlist = row_id(1, 0);
        insert(
            &db,
            "playlists",
            playlist,
            BTreeMap::from([("name".into(), Value::String("Road tape".into()))]),
        );
        for index in 0..track_count {
            let album = row_id(2, index / 8);
            if index % 8 == 0 {
                insert(
                    &db,
                    "albums",
                    album,
                    BTreeMap::from([
                        (
                            "title".into(),
                            Value::String(format!("Album {:04}", index / 8)),
                        ),
                        ("artist".into(), Value::String("The Local Hosts".into())),
                    ]),
                );
            }
            let track = row_id(3, index);
            insert(
                &db,
                "tracks",
                track,
                BTreeMap::from([
                    ("album_id".into(), Value::Uuid(album.0)),
                    ("title".into(), Value::String(format!("Track {index:05}"))),
                    ("ordinal".into(), Value::I32((index % 8) as i32)),
                    ("duration_ms".into(), Value::I32(180_000)),
                ]),
            );
            insert(
                &db,
                "playlist_entries",
                row_id(4, index),
                BTreeMap::from([
                    ("playlist_id".into(), Value::Uuid(playlist.0)),
                    ("track_id".into(), Value::Uuid(track.0)),
                    ("position".into(), Value::F64(index as f64)),
                ]),
            );
        }
        let coverflow = db
            .prepare_query(
                &Query::from("albums")
                    .order_by("title", OrderDirection::Asc)
                    .limit(20),
            )
            .expect("prepare CoverFlow query");
        let playlist_window = db
            .prepare_query(
                &Query::from("playlist_entries")
                    .filter(eq(col("playlist_id"), lit(playlist.0)))
                    .order_by("position", OrderDirection::Asc)
                    .offset(8)
                    .limit(16),
            )
            .expect("prepare playlist window");
        Self {
            db,
            coverflow,
            playlist_window,
        }
    }
    pub fn coverflow_count(&self) -> usize {
        self.db.read(&self.coverflow).expect("read albums").len()
    }
    pub fn playlist_window_count(&self) -> usize {
        self.db
            .read(&self.playlist_window)
            .expect("read playlist window")
            .len()
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("albums")
                    .column("title", ColumnType::Text)
                    .column("artist", ColumnType::Text)
                    .index_only(["title"]),
            )
            .table(
                TableSchemaBuilder::new("tracks")
                    .fk_column("album_id", "albums")
                    .column("title", ColumnType::Text)
                    .column("ordinal", ColumnType::Integer)
                    .column("duration_ms", ColumnType::Integer)
                    .index_only(["album_id", "ordinal"]),
            )
            .table(TableSchemaBuilder::new("playlists").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("playlist_entries")
                    .fk_column("playlist_id", "playlists")
                    .fk_column("track_id", "tracks")
                    .column("position", ColumnType::Double)
                    .index_only(["playlist_id", "position"]),
            )
            .build(),
    )
    .expect("RecordPlayer benchmark schema compiles")
}
fn insert(db: &BenchDb, table: &str, id: RowUuid, cells: BTreeMap<String, Value>) {
    let write = block_on(db.insert_with_id_attributed(AuthorSubject::SYSTEM, table, id, cells))
        .expect("insert fixture row");
    block_on(write.wait(DurabilityTier::Local)).expect("fixture durable");
}
fn row_id(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}
