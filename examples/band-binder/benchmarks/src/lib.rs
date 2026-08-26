//! Self-contained BandBinder recursive-page and ordered-block workloads.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, PreparedQuery, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, all_of, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

type BenchDb = Db<MemoryStorage>;

/// Deterministic fixture for a nested page hierarchy and heterogeneous blocks.
pub struct Fixture {
    db: BenchDb,
    sibling_window: PreparedQuery,
    child_pages: PreparedQuery,
    task_window: PreparedQuery,
    calendar_window: PreparedQuery,
    song_window: PreparedQuery,
    suggestion_window: PreparedQuery,
    attachment_window: PreparedQuery,
}

impl Fixture {
    pub fn new(block_count: usize) -> Self {
        assert!(block_count >= 32, "fixture needs a useful sibling window");
        let schema = schema();
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let db = block_on(Db::open(DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0xbb; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .expect("open BandBinder benchmark database");

        let workspace = row_id(1, 0);
        let root = row_id(2, 0);
        insert(
            &db,
            "workspaces",
            workspace,
            BTreeMap::from([("name".into(), Value::String("BandBinder".into()))]),
        );
        insert(
            &db,
            "pages",
            root,
            BTreeMap::from([
                ("workspaceId".into(), Value::Uuid(workspace.0)),
                ("title".into(), Value::String("Tour book".into())),
            ]),
        );
        for index in 0..16 {
            insert(
                &db,
                "pages",
                row_id(2, index + 1),
                BTreeMap::from([
                    ("workspaceId".into(), Value::Uuid(workspace.0)),
                    (
                        "parentPageId".into(),
                        Value::Nullable(Some(Box::new(Value::Uuid(root.0)))),
                    ),
                    (
                        "title".into(),
                        Value::String(format!("Child page {index:02}")),
                    ),
                ]),
            );
        }
        for index in 0..block_count {
            insert(
                &db,
                "blocks",
                row_id(3, index),
                BTreeMap::from([
                    ("workspaceId".into(), Value::Uuid(workspace.0)),
                    ("pageId".into(), Value::Uuid(root.0)),
                    ("position".into(), Value::F64(index as f64)),
                    (
                        "kind".into(),
                        Value::String(["text", "song", "task", "calendar"][index % 4].into()),
                    ),
                    (
                        "payload".into(),
                        Value::String(format!("{{\"text\":\"Block {index:05}\"}}")),
                    ),
                ]),
            );
        }
        for index in 0..32 {
            let block = row_id(3, index);
            insert(
                &db,
                "tasks",
                row_id(4, index),
                BTreeMap::from([
                    ("workspaceId".into(), Value::Uuid(workspace.0)),
                    ("blockId".into(), Value::Uuid(block.0)),
                    ("title".into(), Value::String(format!("Task {index:02}"))),
                    ("completed".into(), Value::Bool(index % 3 == 0)),
                    (
                        "dueAt".into(),
                        Value::Nullable(Some(Box::new(Value::U64(
                            1_800_000_000_000_000 + index as u64,
                        )))),
                    ),
                ]),
            );
            insert(
                &db,
                "calendarEvents",
                row_id(5, index),
                BTreeMap::from([
                    ("workspaceId".into(), Value::Uuid(workspace.0)),
                    ("blockId".into(), Value::Uuid(block.0)),
                    ("title".into(), Value::String(format!("Show {index:02}"))),
                    (
                        "startsAt".into(),
                        Value::U64(1_800_000_000_000_000 + index as u64),
                    ),
                    (
                        "endsAt".into(),
                        Value::U64(1_800_003_600_000_000 + index as u64),
                    ),
                ]),
            );
            insert(
                &db,
                "songs",
                row_id(6, index),
                BTreeMap::from([
                    ("workspaceId".into(), Value::Uuid(workspace.0)),
                    ("blockId".into(), Value::Uuid(block.0)),
                    ("title".into(), Value::String(format!("Song {index:02}"))),
                ]),
            );
            insert(
                &db,
                "suggestions",
                row_id(7, index),
                BTreeMap::from([
                    ("workspaceId".into(), Value::Uuid(workspace.0)),
                    // A real suggestion surface is a bounded window for one
                    // block, not one suggestion from every block.
                    ("blockId".into(), Value::Uuid(row_id(3, 0).0)),
                    (
                        "payload".into(),
                        Value::String(format!("{{\"replacement\":\"Verse {index:02}\"}}")),
                    ),
                    ("status".into(), Value::String("open".into())),
                ]),
            );
            insert(
                &db,
                "attachments",
                row_id(8, index),
                BTreeMap::from([
                    ("workspaceId".into(), Value::Uuid(workspace.0)),
                    ("blockId".into(), Value::Uuid(row_id(3, 0).0)),
                    (
                        "name".into(),
                        Value::String(format!("chart-{index:02}.pdf")),
                    ),
                    ("mediaType".into(), Value::String("application/pdf".into())),
                    ("bytes".into(), Value::Bytes(vec![index as u8; 64])),
                ]),
            );
        }
        let sibling_window = db
            .prepare_query(
                &Query::from("blocks")
                    .filter(all_of([
                        eq(col("workspaceId"), lit(workspace.0)),
                        eq(col("pageId"), lit(root.0)),
                    ]))
                    .order_by("position", OrderDirection::Asc)
                    .offset(8)
                    .limit(16),
            )
            .expect("prepare ordered sibling page");
        let child_pages = db
            .prepare_query(
                &Query::from("pages")
                    .filter(all_of([
                        eq(col("workspaceId"), lit(workspace.0)),
                        eq(col("parentPageId"), lit(root.0)),
                    ]))
                    .order_by("title", OrderDirection::Asc),
            )
            .expect("prepare child page traversal");
        let task_window = bounded_workspace_query(&db, "tasks", "dueAt", workspace);
        let calendar_window = bounded_workspace_query(&db, "calendarEvents", "startsAt", workspace);
        let song_window = bounded_workspace_query(&db, "songs", "title", workspace);
        let suggestion_window = db
            .prepare_query(
                &Query::from("suggestions")
                    .filter(all_of([
                        eq(col("workspaceId"), lit(workspace.0)),
                        eq(col("blockId"), lit(row_id(3, 0).0)),
                        eq(col("status"), lit("open")),
                    ]))
                    .select(["payload", "status", "$createdAt"])
                    .order_by("$createdAt", OrderDirection::Asc)
                    .limit(12),
            )
            .expect("prepare live suggestion surface");
        let attachment_window = db
            .prepare_query(
                &Query::from("attachments")
                    .filter(all_of([
                        eq(col("workspaceId"), lit(workspace.0)),
                        eq(col("blockId"), lit(row_id(3, 0).0)),
                    ]))
                    .order_by("name", OrderDirection::Asc)
                    .limit(12),
            )
            .expect("prepare block attachment surface");
        Self {
            db,
            sibling_window,
            child_pages,
            task_window,
            calendar_window,
            song_window,
            suggestion_window,
            attachment_window,
        }
    }

    pub fn sibling_window_count(&self) -> usize {
        self.db
            .read(&self.sibling_window)
            .expect("read sibling window")
            .len()
    }

    pub fn child_page_count(&self) -> usize {
        self.db
            .read(&self.child_pages)
            .expect("read child pages")
            .len()
    }

    pub fn surface_window_counts(&self) -> [usize; 4] {
        [
            self.db.read(&self.task_window).expect("read tasks").len(),
            self.db
                .read(&self.calendar_window)
                .expect("read calendar")
                .len(),
            self.db.read(&self.song_window).expect("read songs").len(),
            self.db
                .read(&self.attachment_window)
                .expect("read attachments")
                .len(),
        ]
    }

    pub fn suggestion_window_count(&self) -> usize {
        self.db
            .read(&self.suggestion_window)
            .expect("read live suggestion window")
            .len()
    }
}

fn bounded_workspace_query(
    db: &BenchDb,
    table: &str,
    order: &str,
    workspace: RowUuid,
) -> PreparedQuery {
    db.prepare_query(
        &Query::from(table)
            .filter(eq(col("workspaceId"), lit(workspace.0)))
            .order_by(order, OrderDirection::Asc)
            .limit(12),
    )
    .expect("prepare bounded workspace surface")
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("workspaces").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("workspaceId", "workspaces")
                    .column("subject", ColumnType::Text)
                    .column("role", ColumnType::Text)
                    .index_only(["workspaceId", "subject", "role"]),
            )
            .table(
                TableSchemaBuilder::new("pages")
                    .fk_column("workspaceId", "workspaces")
                    .nullable_fk_column("parentPageId", "pages")
                    .column("title", ColumnType::Text)
                    .index_only(["workspaceId", "parentPageId", "title"]),
            )
            .table(
                TableSchemaBuilder::new("blocks")
                    .fk_column("workspaceId", "workspaces")
                    .fk_column("pageId", "pages")
                    .nullable_fk_column("parentBlockId", "blocks")
                    .column("position", ColumnType::Double)
                    .column("kind", ColumnType::Text)
                    .column("payload", ColumnType::Json { schema: None })
                    .index_only(["workspaceId", "pageId", "position"]),
            )
            .table(
                TableSchemaBuilder::new("tasks")
                    .fk_column("workspaceId", "workspaces")
                    .fk_column("blockId", "blocks")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean)
                    .nullable_column("assigneeSubject", ColumnType::Text)
                    .nullable_column("dueAt", ColumnType::Timestamp)
                    .index_only(["workspaceId", "dueAt", "blockId"]),
            )
            .table(
                TableSchemaBuilder::new("calendarEvents")
                    .fk_column("workspaceId", "workspaces")
                    .fk_column("blockId", "blocks")
                    .column("title", ColumnType::Text)
                    .column("startsAt", ColumnType::Timestamp)
                    .column("endsAt", ColumnType::Timestamp)
                    .index_only(["workspaceId", "startsAt", "blockId"]),
            )
            .table(
                TableSchemaBuilder::new("songs")
                    .fk_column("workspaceId", "workspaces")
                    .fk_column("blockId", "blocks")
                    .column("title", ColumnType::Text)
                    .nullable_column("key", ColumnType::Text)
                    .nullable_column("bpm", ColumnType::Double)
                    .index_only(["workspaceId", "title", "blockId"]),
            )
            .table(
                TableSchemaBuilder::new("suggestions")
                    .fk_column("workspaceId", "workspaces")
                    .fk_column("blockId", "blocks")
                    .column("payload", ColumnType::Json { schema: None })
                    .column("status", ColumnType::Text)
                    .index_only(["workspaceId", "blockId", "status"]),
            )
            .table(
                TableSchemaBuilder::new("attachments")
                    .fk_column("workspaceId", "workspaces")
                    .fk_column("blockId", "blocks")
                    .column("name", ColumnType::Text)
                    .column("mediaType", ColumnType::Text)
                    .column("bytes", ColumnType::Bytea)
                    .index_only(["workspaceId", "blockId", "name"]),
            )
            .build(),
    )
    .expect("BandBinder benchmark schema compiles")
}

fn insert(db: &BenchDb, table: &str, id: RowUuid, cells: BTreeMap<String, Value>) {
    let write = block_on(db.insert_with_id_attributed(AuthorSubject::SYSTEM, table, id, cells))
        .expect("insert fixture row");
    block_on(write.wait(DurabilityTier::Local)).expect("fixture row reaches local durability");
}

fn row_id(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}
