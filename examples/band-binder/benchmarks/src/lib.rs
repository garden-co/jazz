//! Self-contained BandBinder recursive-page and ordered-block workloads.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, PreparedQuery, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

type BenchDb = Db<MemoryStorage>;

/// Deterministic fixture for a nested page hierarchy and heterogeneous blocks.
pub struct Fixture {
    db: BenchDb,
    sibling_window: PreparedQuery,
    child_pages: PreparedQuery,
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
                ("workspace".into(), Value::Uuid(workspace.0)),
                ("title".into(), Value::String("Tour book".into())),
                ("branch".into(), Value::String("main".into())),
            ]),
        );
        for index in 0..16 {
            insert(
                &db,
                "pages",
                row_id(2, index + 1),
                BTreeMap::from([
                    ("workspace".into(), Value::Uuid(workspace.0)),
                    ("parent_page".into(), Value::Uuid(root.0)),
                    (
                        "title".into(),
                        Value::String(format!("Child page {index:02}")),
                    ),
                    ("branch".into(), Value::String("main".into())),
                ]),
            );
        }
        for index in 0..block_count {
            insert(
                &db,
                "blocks",
                row_id(3, index),
                BTreeMap::from([
                    ("page".into(), Value::Uuid(root.0)),
                    ("position".into(), Value::F64(index as f64)),
                    (
                        "kind".into(),
                        Value::String(["text", "song", "task", "calendar"][index % 4].into()),
                    ),
                    ("body".into(), Value::String(format!("Block {index:05}"))),
                ]),
            );
        }
        let sibling_window = db
            .prepare_query(
                &Query::from("blocks")
                    .filter(eq(col("page"), lit(root.0)))
                    .order_by("position", OrderDirection::Asc)
                    .offset(8)
                    .limit(16),
            )
            .expect("prepare ordered sibling page");
        let child_pages = db
            .prepare_query(
                &Query::from("pages")
                    .filter(eq(col("parent_page"), lit(root.0)))
                    .order_by("title", OrderDirection::Asc),
            )
            .expect("prepare child page traversal");
        Self {
            db,
            sibling_window,
            child_pages,
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
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("workspaces").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("pages")
                    .fk_column("workspace", "workspaces")
                    .fk_column("parent_page", "pages")
                    .column("title", ColumnType::Text)
                    .column("branch", ColumnType::Text)
                    .index_only(["parent_page", "title"]),
            )
            .table(
                TableSchemaBuilder::new("blocks")
                    .fk_column("page", "pages")
                    .column("position", ColumnType::Double)
                    .column("kind", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .index_only(["page", "position", "kind"]),
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
