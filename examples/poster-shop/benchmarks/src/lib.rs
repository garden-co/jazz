//! Self-contained PosterShop fixture and canvas-shaped query workloads.
//!
//! The Rust model intentionally duplicates the app's canvas/layer/shape and
//! cursor metadata surface. It measures query planning and materialization, not
//! a UI renderer or a vendor-specific canvas package.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, PreparedQuery, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::CurrentRow;
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

const EDITORS: usize = 8;
type BenchDb = Db<MemoryStorage>;

pub struct Fixture {
    db: BenchDb,
    shapes: TableSchema,
    ordered_shapes: PreparedQuery,
    cursor_fanout: PreparedQuery,
    layer_shapes: PreparedQuery,
}

impl Fixture {
    pub fn new(shape_count: usize) -> Self {
        assert!(shape_count >= EDITORS);
        let (db, shapes) = open_db();
        insert(
            &db,
            "canvases",
            row_id(1, 0),
            BTreeMap::from([("title".into(), Value::String("Poster".into()))]),
        );
        for layer in 0..4 {
            insert(
                &db,
                "layers",
                row_id(2, layer),
                BTreeMap::from([
                    ("canvas".into(), Value::Uuid(row_id(1, 0).0)),
                    ("z_index".into(), Value::I32(layer as i32)),
                ]),
            );
        }
        for editor in 0..EDITORS {
            insert(
                &db,
                "cursors",
                row_id(3, editor),
                BTreeMap::from([
                    ("canvas".into(), Value::Uuid(row_id(1, 0).0)),
                    ("editor".into(), Value::I32(editor as i32)),
                    ("x".into(), Value::I32(editor as i32)),
                    ("y".into(), Value::I32((editor * 2) as i32)),
                ]),
            );
        }
        for shape in 0..shape_count {
            insert(
                &db,
                "shapes",
                row_id(4, shape),
                BTreeMap::from([
                    ("canvas".into(), Value::Uuid(row_id(1, 0).0)),
                    ("layer".into(), Value::Uuid(row_id(2, shape % 4).0)),
                    ("z_index".into(), Value::I32(shape as i32)),
                    ("x".into(), Value::I32(shape as i32)),
                    ("y".into(), Value::I32((shape / 4) as i32)),
                    ("kind".into(), Value::String("rect".into())),
                ]),
            );
        }
        let ordered_shapes = db
            .prepare_query(
                &Query::from("shapes")
                    .filter(eq(col("canvas"), lit(row_id(1, 0).0)))
                    .order_by("z_index", OrderDirection::Asc),
            )
            .expect("prepare ordered shapes");
        let cursor_fanout = db
            .prepare_query(
                &Query::from("cursors")
                    .filter(eq(col("canvas"), lit(row_id(1, 0).0)))
                    .order_by("editor", OrderDirection::Asc),
            )
            .expect("prepare cursor fanout");
        let layer_shapes = db
            .prepare_query(
                &Query::from("shapes")
                    .filter(eq(col("layer"), lit(row_id(2, 0).0)))
                    .order_by("z_index", OrderDirection::Asc),
            )
            .expect("prepare layer shapes");
        Self {
            db,
            shapes,
            ordered_shapes,
            cursor_fanout,
            layer_shapes,
        }
    }

    pub fn ordered_shape_count(&self) -> usize {
        self.read(&self.ordered_shapes).len()
    }
    pub fn cursor_fanout_count(&self) -> usize {
        self.read(&self.cursor_fanout).len()
    }
    pub fn layer_shape_count(&self) -> usize {
        self.read(&self.layer_shapes).len()
    }
    pub fn ordered_z_indices(&self) -> Vec<i32> {
        self.read(&self.ordered_shapes)
            .into_iter()
            .map(|row| match row.cell(&self.shapes, "z_index") {
                Some(Value::I32(value)) => value,
                value => panic!("unexpected z_index: {value:?}"),
            })
            .collect()
    }
    fn read(&self, query: &PreparedQuery) -> Vec<CurrentRow> {
        self.db.read(query).expect("PosterShop benchmark read")
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("canvases").column("title", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("layers")
                    .fk_column("canvas", "canvases")
                    .column("z_index", ColumnType::Integer)
                    .index_only(["canvas", "z_index"]),
            )
            .table(
                TableSchemaBuilder::new("shapes")
                    .fk_column("canvas", "canvases")
                    .fk_column("layer", "layers")
                    .column("z_index", ColumnType::Integer)
                    .column("x", ColumnType::Integer)
                    .column("y", ColumnType::Integer)
                    .column("kind", ColumnType::Text)
                    .index_only(["canvas", "layer", "z_index"]),
            )
            .table(
                TableSchemaBuilder::new("cursors")
                    .fk_column("canvas", "canvases")
                    .column("editor", ColumnType::Integer)
                    .column("x", ColumnType::Integer)
                    .column("y", ColumnType::Integer)
                    .index_only(["canvas", "editor"]),
            )
            .build(),
    )
    .expect("PosterShop schema compiles")
}

fn open_db() -> (BenchDb, TableSchema) {
    let schema = schema();
    let shapes = schema
        .tables()
        .iter()
        .find(|table| table.name == "shapes")
        .expect("shapes table")
        .clone();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        MemoryStorage::new(&family_refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0x51; 16]),
            author: AuthorId::SYSTEM,
        },
    )))
    .expect("open PosterShop benchmark");
    (db, shapes)
}

fn insert(db: &BenchDb, table: &str, id: RowUuid, cells: BTreeMap<String, Value>) {
    let write = block_on(db.insert_with_id(table, id, cells)).expect("seed PosterShop row");
    block_on(write.wait(DurabilityTier::Local)).expect("local fixture write");
}
fn row_id(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}
