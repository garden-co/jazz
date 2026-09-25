//! Self-contained RecordPlayer metadata and playlist-window workloads.

use jazz::db::{
    Db, DbConfig, DbIdentity, PreparedQuery, ReadOpts, SubscriptionEvent, SubscriptionStream,
    block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::cell::Cell;
use std::collections::BTreeMap;

type BenchDb = Db<MemoryStorage>;

/// Deterministic record catalogue and one ordered playlist.
pub struct Fixture {
    db: BenchDb,
    coverflow: PreparedQuery,
    track_metadata: PreparedQuery,
    playlist_window: PreparedQuery,
    track_count: usize,
    added_entries: Cell<usize>,
}

/// The visible playlist window a listener keeps subscribed.
pub struct LivePlaylist {
    window: SubscriptionStream,
}

impl Fixture {
    pub fn new(track_count: usize) -> Self {
        assert!(track_count >= 32, "fixture needs a useful playlist window");
        let schema = schema();
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let db = block_on(Db::open(DbConfig::new(
            schema,
            MemoryStorage::new(&refs).expect("valid memory storage families"),
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
            let mut track_cells = BTreeMap::from([
                ("album_id".into(), Value::Uuid(album.0)),
                ("title".into(), Value::String(format!("Track {index:05}"))),
                ("ordinal".into(), Value::I32((index % 8) as i32)),
                ("duration_ms".into(), Value::I32(180_000)),
            ]);
            // The metadata workload must not accidentally depend on a small
            // scalar fixture: the first album carries realistic nullable audio
            // bytes while its query projects only metadata.
            if index < 8 {
                track_cells.insert(
                    "audio_bytes".into(),
                    Value::Nullable(Some(Box::new(Value::Bytes(vec![0x52; 64 * 1024])))),
                );
            }
            insert(&db, "tracks", track, track_cells);
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
        let track_metadata = db
            .prepare_query(
                &Query::from("tracks")
                    .filter(eq(col("album_id"), lit(row_id(2, 0).0)))
                    .order_by("ordinal", OrderDirection::Asc)
                    .limit(32)
                    .select(["album_id", "title", "ordinal", "duration_ms"]),
            )
            .expect("prepare metadata-only track query");
        Self {
            db,
            coverflow,
            track_metadata,
            playlist_window,
            track_count,
            added_entries: Cell::new(0),
        }
    }

    /// Opening the library: subscribe to the 20-album CoverFlow shelf and the
    /// focused album's metadata-only track list, and receive both first
    /// results. Returns the rows delivered.
    pub fn open_coverflow(&self) -> usize {
        self.open(&self.coverflow) + self.open(&self.track_metadata)
    }

    /// Opening a playlist: subscribe to its visible 16-entry window and
    /// receive the first result.
    pub fn open_playlist(&self) -> usize {
        self.open(&self.playlist_window)
    }

    pub fn live_playlist(&self) -> LivePlaylist {
        let mut window = self.subscribe(&self.playlist_window);
        assert_eq!(initial_rows(&mut window), 16);
        LivePlaylist { window }
    }

    /// Adds a track inside the visible playlist window and waits until the
    /// live window shows it. Returns the number of rows the window added.
    pub fn add_to_playlist(&self, live: &mut LivePlaylist) -> usize {
        let added = self.added_entries.get();
        self.added_entries.set(added + 1);
        // Between the first and second visible entries, so every addition is
        // visible; fractional positions mirror the app's ordering keys.
        let position = 8.0 + 1.0 / (added as f64 + 2.0);
        insert(
            &self.db,
            "playlist_entries",
            row_id(5, added),
            BTreeMap::from([
                ("playlist_id".into(), Value::Uuid(row_id(1, 0).0)),
                (
                    "track_id".into(),
                    Value::Uuid(row_id(3, added % self.track_count).0),
                ),
                ("position".into(), Value::F64(position)),
            ]),
        );
        match block_on(live.window.next_event()).expect("playlist window observes the addition") {
            SubscriptionEvent::Delta { added, .. } => added.len(),
            event => panic!("unexpected playlist event: {event:?}"),
        }
    }

    fn open(&self, query: &PreparedQuery) -> usize {
        initial_rows(&mut self.subscribe(query))
    }

    fn subscribe(&self, query: &PreparedQuery) -> SubscriptionStream {
        block_on(self.db.subscribe(query, ReadOpts::default()))
            .expect("open RecordPlayer subscription")
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

    pub fn track_metadata_count(&self) -> usize {
        self.db
            .read(&self.track_metadata)
            .expect("read track metadata")
            .len()
    }

    pub fn track_metadata_projection(&self) -> &[String] {
        self.track_metadata
            .shape()
            .query()
            .select
            .as_deref()
            .expect("metadata query is projected")
    }

    pub fn indexed_access_paths() -> BTreeMap<String, Vec<String>> {
        schema()
            .public_schema()
            .iter()
            .filter_map(|(name, table)| {
                table.indexed_columns.as_ref().map(|columns| {
                    (
                        name.as_str().to_owned(),
                        columns
                            .iter()
                            .map(|column| column.as_str().to_owned())
                            .collect(),
                    )
                })
            })
            .collect()
    }
}

fn initial_rows(stream: &mut SubscriptionStream) -> usize {
    match block_on(stream.next_event()).expect("subscription has an initial result") {
        SubscriptionEvent::Delta {
            reset: true, added, ..
        } => added.len(),
        event => panic!("unexpected initial RecordPlayer event: {event:?}"),
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
                    .nullable_column("audio_bytes", ColumnType::Bytea)
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
    let write = block_on(db.insert(
        table,
        cells,
        jazz::db::InsertOptions {
            row_id: Some(id),
            identity: jazz::db::WriteIdentity::Attribution(AuthorSubject::SYSTEM),
            ..Default::default()
        },
    ))
    .expect("insert fixture row");
    block_on(write.wait(DurabilityTier::Local)).expect("fixture durable");
}
fn row_id(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}
