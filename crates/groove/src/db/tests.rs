//! End-to-end behavior guards for the database facade and IVM integration.
//!
//! These tests own broad public-surface coverage: commits, queries,
//! subscriptions, joins, recursion, indices, prepared shapes, and persistence
//! through the [`super::Database`] API. Lower-level record-layout tests live in
//! [`crate::records::tests`]; runtime-specific regression tests live near the
//! runtime module.

use super::*;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::mpsc::TryRecvError;
use std::time::Instant;

use crate::ivm::{
    AggregateExpr, AggregateFunction, IvmRuntimeError, LiteralValue, PlanExpr, PredicateExpr,
    ProjectField, StaticScanSpec, TopByLimit, TopByOrder,
};
use crate::queries::{
    BinaryOp, ColumnRef, Cte, Expr, JoinConstraint, JoinKind, Query, Select, SelectItem, TableRef,
    UnaryOp, WithQuery,
};
use crate::records::{EnumSchema, RecordDescriptor, ValueType};
use crate::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, DirectRecordStoreSchema, IndexSchema, IntegerKeyType,
    PrimaryKey, PrimaryKeyColumn, PrimaryKeyType,
};
use crate::storage::{
    ColumnFamilyName, Error as StorageError, Key, KeyValue, MemoryStorage, OrderedKvStorage,
    RocksDbStorage, ScanVisitor, StorageLayout, Value as StorageValue, WriteOperation,
};
use crate::window_codec::TARGET_RECORDS_PER_WINDOW;

fn albums_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn indexed_albums_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("albums_by_title", ["title"]))])
}

fn unique_indexed_albums_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("unique_albums_by_title", ["title"]).unique())])
}

fn scan_spec_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "docs",
        [
            ColumnSchema::new("tenant", ColumnType::String),
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("path", ColumnType::String),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::new("tenant", PrimaryKeyType::String),
        PrimaryKeyColumn::integer("id", IntegerKeyType::U64),
    ]))
    .with_index(IndexSchema::new("docs_by_path", ["path", "tenant"]))])
}

fn insert_scan_doc(batch: &mut DatabaseBatch, tenant: &str, id: u64, path: &str, payload: &[u8]) {
    batch.insert(
        "docs",
        vec![
            Value::String(tenant.to_owned()),
            Value::U64(id),
            Value::String(path.to_owned()),
            Value::Bytes(payload.to_vec()),
        ],
    );
}

fn uuid(value: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(value)
}

fn indexed_tracks_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "tracks",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("album_id", ColumnType::U64),
            ColumnSchema::new("disc", ColumnType::U64.nullable()),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new(
        "tracks_by_album_disc",
        ["album_id", "disc"],
    ))
    .with_index(IndexSchema::new("tracks_by_title_unique", ["title"]).unique())])
}

fn track_values(id: u64, album_id: u64, disc: Option<u64>, title: &str) -> Vec<Value> {
    vec![
        Value::U64(id),
        Value::U64(album_id),
        Value::Nullable(disc.map(|disc| Box::new(Value::U64(disc)))),
        Value::String(title.to_owned()),
    ]
}

fn history_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "history",
            [
                ColumnSchema::new("row", ColumnType::U64),
                ColumnSchema::new("stamp", ColumnType::U64),
                ColumnSchema::new("node", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::integer("row", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("stamp", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("node", IntegerKeyType::U64),
        ])),
        TableSchema::new(
            "rows",
            [
                ColumnSchema::new("row", ColumnType::U64),
                ColumnSchema::new("label", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("row", IntegerKeyType::U64)),
        TableSchema::new("blockers", [ColumnSchema::new("row", ColumnType::U64)])
            .with_primary_key(PrimaryKey::new("row", IntegerKeyType::U64)),
    ])
}

fn two_history_tables_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "history",
            [
                ColumnSchema::new("row", ColumnType::U64),
                ColumnSchema::new("stamp", ColumnType::U64),
                ColumnSchema::new("node", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::integer("row", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("stamp", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("node", IntegerKeyType::U64),
        ])),
        TableSchema::new(
            "history_shadow",
            [
                ColumnSchema::new("row", ColumnType::U64),
                ColumnSchema::new("stamp", ColumnType::U64),
                ColumnSchema::new("node", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::integer("row", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("stamp", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("node", IntegerKeyType::U64),
        ])),
    ])
}

fn history_values(row: u64, stamp: u64, node: u64, title: &str) -> Vec<Value> {
    vec![
        Value::U64(row),
        Value::U64(stamp),
        Value::U64(node),
        Value::String(title.to_owned()),
    ]
}

fn history_key(row: u64, stamp: u64, node: u64) -> PrimaryKeyValue {
    PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::U64(row),
        PrimaryKeyValue::U64(stamp),
        PrimaryKeyValue::U64(node),
    ])
}

fn history_arg_max() -> GraphBuilder {
    GraphBuilder::arg_max_by(GraphBuilder::table("history"), ["row"], ["stamp", "node"])
}

fn history_arg_min() -> GraphBuilder {
    GraphBuilder::arg_min_by(GraphBuilder::table("history"), ["row"], ["stamp", "node"])
}

fn history_top_by_stamp_asc(limit: u64) -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("history"),
        ["row"],
        [TopByOrder::asc("stamp")],
        ["node"],
        0,
        TopByLimit::Finite(limit),
    )
}

fn history_top_by_stamp_desc(limit: u64) -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("history"),
        ["row"],
        [TopByOrder::desc("stamp")],
        ["node"],
        0,
        TopByLimit::Finite(limit),
    )
}

fn history_top_by_stamp_asc_offset(offset: u64, limit: u64) -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("history"),
        ["row"],
        [TopByOrder::asc("stamp")],
        ["node"],
        offset,
        TopByLimit::Finite(limit),
    )
}

fn nullable_scores_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "scores",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("score", ColumnType::U64.nullable()),
            ColumnSchema::new("label", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn uuid_docs_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "docs",
        [
            ColumnSchema::new("id", ColumnType::Uuid),
            ColumnSchema::new("owner", ColumnType::Uuid.nullable()),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::composite([PrimaryKeyColumn::uuid("id")]))
    .with_index(IndexSchema::new("docs_by_owner", ["owner", "id"]))])
}

fn nullable_routed_docs_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "docs",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("owner", ColumnType::Uuid.nullable()),
            ColumnSchema::new("tag", ColumnType::String.nullable()),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn enum_tasks_schema() -> DatabaseSchema {
    let status =
        ColumnType::Enum(EnumSchema::new("task_status", ["todo", "doing", "done"]).unwrap());
    DatabaseSchema::new([TableSchema::new(
        "tasks",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("status", status.clone()),
            ColumnSchema::new("maybe_status", status.nullable()),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("tasks_by_status", ["status"]))])
}

fn tuple_edges_schema() -> DatabaseSchema {
    let tx_ref = ColumnType::Tuple(vec![ColumnType::Uuid, ColumnType::U64]);
    DatabaseSchema::new([TableSchema::new(
        "edges",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("parent", tx_ref.clone()),
            ColumnSchema::new("maybe_parent", tx_ref.nullable()),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("edges_by_parent", ["parent"]))])
}

fn interval_history_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "history",
        [
            ColumnSchema::new("row_uuid", ColumnType::Bytes),
            ColumnSchema::new("tx_node_id", ColumnType::U64),
            ColumnSchema::new("tx_local_seq", ColumnType::U64),
            ColumnSchema::new("until", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::bytes("row_uuid"),
        PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("tx_local_seq", IntegerKeyType::U64),
    ]))
    .with_index(IndexSchema::new(
        "history_by_until_row",
        ["until", "row_uuid"],
    ))])
}

fn nullable_markers_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "markers",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("marker", ColumnType::String.nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn nested_nullable_markers_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "markers",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("marker", ColumnType::String.nullable().nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn two_album_tables_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "archived_albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn albums_artists_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "artists",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("name", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn files_parts_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "files",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("part_ids", ColumnType::Uuid.array_of()),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "file_parts",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("part_uuid", ColumnType::Uuid),
                ColumnSchema::new("data", ColumnType::Bytes),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn indexed_files_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "files",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("part_ids", ColumnType::Uuid.array_of()),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("files_by_part_ids", ["part_ids"]))])
}

fn nullable_files_parts_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "files",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("part_ids", ColumnType::Uuid.array_of().nullable()),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "file_parts",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("part_uuid", ColumnType::Uuid.nullable()),
                ColumnSchema::new("data", ColumnType::Bytes),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn albums_blockers_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "blocks",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn tenant_albums_artists_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "albums",
            [
                ColumnSchema::new("tenant_id", ColumnType::U64),
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "artists",
            [
                ColumnSchema::new("tenant_id", ColumnType::U64),
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("name", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn edges_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "edges",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("src", ColumnType::U64),
            ColumnSchema::new("dst", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn edges_docs_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "edges",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("src", ColumnType::U64),
                ColumnSchema::new("dst", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "docs",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("team", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn edges_blockers_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "edges",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("src", ColumnType::U64),
                ColumnSchema::new("dst", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "blockers",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("src", ColumnType::U64),
                ColumnSchema::new("dst", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn integer_key_widths_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new("u8_keys", [ColumnSchema::new("id", ColumnType::U8)])
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U8)),
        TableSchema::new("u16_keys", [ColumnSchema::new("id", ColumnType::U16)])
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U16)),
        TableSchema::new("u32_keys", [ColumnSchema::new("id", ColumnType::U32)])
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U32)),
        TableSchema::new("u64_keys", [ColumnSchema::new("id", ColumnType::U64)])
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn composite_key_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "history",
        [
            ColumnSchema::new("row_uuid", ColumnType::Bytes),
            ColumnSchema::new("tx_node_id", ColumnType::U64),
            ColumnSchema::new("tx_local_epoch", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::bytes("row_uuid"),
        PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("tx_local_epoch", IntegerKeyType::U64),
    ]))])
}

fn expect_recv_vals(subscription: &Subscription) -> Vec<(Vec<Value>, i64)> {
    loop {
        let deltas = subscription.recv().unwrap();
        if !deltas.is_empty() {
            let mut values = deltas.to_values().unwrap();
            values.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            return values;
        }
    }
}

fn expect_try_recv_vals(subscription: &Subscription) -> Vec<(Vec<Value>, i64)> {
    for _ in 0..100 {
        if let Ok(deltas) = subscription.try_recv()
            && !deltas.is_empty()
        {
            let mut values = deltas.to_values().unwrap();
            values.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            return values;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    panic!("expected subscription notification");
}

fn col(name: &str) -> Expr {
    Expr::column(name)
}

fn qcol(qualifier: &str, name: &str) -> Expr {
    Expr::Column(ColumnRef::qualified([qualifier], name))
}

fn select_query(select: Select) -> Query {
    Query::Select(Box::new(select))
}

fn reachability_graph(max_iters: usize) -> GraphBuilder {
    let reach = RecordDescriptor::new([
        ("src", ColumnType::U64.value_type()),
        ("dst", ColumnType::U64.value_type()),
    ]);
    let seed = GraphBuilder::table("edges").project(["src", "dst"]);
    let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
    let frontier = GraphBuilder::frontier_source("frontier", reach);
    let step = GraphBuilder::join(frontier, edge_pairs, ["dst"], ["src"]).project_fields([
        ProjectField::renamed("left.src", "src"),
        ProjectField::renamed("right.dst", "dst"),
    ]);
    GraphBuilder::recursive(seed, step, "frontier", max_iters)
}

fn prepared_reachability_graph(edge_input: GraphBuilder, max_iters: usize) -> GraphBuilder {
    let reach = RecordDescriptor::new([
        ("seed", ColumnType::U64.value_type()),
        ("dst", ColumnType::U64.value_type()),
    ]);
    let seed = GraphBuilder::binding_source(
        "prepared-reach",
        RecordDescriptor::new([("seed", ColumnType::U64.value_type())]),
    )
    .project_fields([
        ProjectField::renamed("seed", "seed"),
        ProjectField::renamed("seed", "dst"),
    ]);
    let frontier = GraphBuilder::frontier_source("frontier", reach);
    let step = GraphBuilder::join(
        frontier,
        edge_input.project(["src", "dst"]),
        ["dst"],
        ["src"],
    )
    .project_fields([
        ProjectField::renamed("left.seed", "seed"),
        ProjectField::renamed("right.dst", "dst"),
    ]);
    GraphBuilder::recursive(seed, step, "frontier", max_iters)
}

fn prepared_reachability_shape(
    database: &mut Database<RocksDbStorage>,
) -> crate::ivm::PreparedShape {
    database
        .prepare_one_sink(
            prepared_reachability_graph(GraphBuilder::table("edges"), 16),
            "prepared-reach",
            RecordDescriptor::new([("seed", ColumnType::U64.value_type())]),
            ["seed".to_owned()],
        )
        .unwrap()
}

fn prepared_reachability_with_antijoin_shape(
    database: &mut Database<RocksDbStorage>,
) -> crate::ivm::PreparedShape {
    let unblocked = GraphBuilder::anti_join(
        GraphBuilder::table("edges"),
        GraphBuilder::table("blockers"),
        ["src", "dst"],
        ["src", "dst"],
    );
    database
        .prepare_one_sink(
            prepared_reachability_graph(unblocked, 16),
            "prepared-reach",
            RecordDescriptor::new([("seed", ColumnType::U64.value_type())]),
            ["seed".to_owned()],
        )
        .unwrap()
}

fn two_hop_graph() -> GraphBuilder {
    let left = GraphBuilder::table("edges").project(["src", "dst"]);
    let right = GraphBuilder::table("edges").project(["src", "dst"]);
    GraphBuilder::join(left, right, ["dst"], ["src"]).project_fields([
        ProjectField::renamed("left.src", "src"),
        ProjectField::renamed("right.dst", "dst"),
    ])
}

fn unblocked_edges_graph() -> GraphBuilder {
    GraphBuilder::anti_join(
        GraphBuilder::table("edges"),
        GraphBuilder::table("blockers"),
        ["src", "dst"],
        ["src", "dst"],
    )
    .project(["src", "dst"])
}

fn artist_album_shape_graph() -> GraphBuilder {
    let params = GraphBuilder::binding_source(
        "artist_params",
        RecordDescriptor::new([("artist_id", ColumnType::U64.value_type())]),
    );
    let albums = GraphBuilder::table("albums").project(["artist_id", "id", "title"]);
    GraphBuilder::join(params, albums, ["artist_id"], ["artist_id"]).project_fields([
        ProjectField::renamed("left.artist_id", "artist_id"),
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
    ])
}

fn artist_binding_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("artist_id", ColumnType::U64.value_type())])
}

fn insert_edge(batch: &mut DatabaseBatch, id: u64, src: u64, dst: u64) {
    batch.insert(
        "edges",
        vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
    );
}

fn grant_shape_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "group_edges",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("src", ColumnType::U64),
                ColumnSchema::new("dst", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "access_edges",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("resource", ColumnType::U64),
                ColumnSchema::new("group", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "resources",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("payload", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn grant_shape_graph() -> GraphBuilder {
    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.value_type())]);
    let reach_descriptor = RecordDescriptor::new([
        ("seed", ColumnType::U64.value_type()),
        ("group", ColumnType::U64.value_type()),
    ]);
    let seed = GraphBuilder::binding_source("grant-claim", binding_descriptor).project_fields([
        ProjectField::renamed("seed", "seed"),
        ProjectField::renamed("seed", "group"),
    ]);
    let frontier = GraphBuilder::frontier_source("frontier", reach_descriptor);
    let step = GraphBuilder::join(
        frontier,
        GraphBuilder::table("group_edges").project(["src", "dst"]),
        ["group"],
        ["src"],
    )
    .project_fields([
        ProjectField::renamed("left.seed", "seed"),
        ProjectField::renamed("right.dst", "group"),
    ]);
    let reach = GraphBuilder::recursive(seed, step, "frontier", 16);
    let visible_access = GraphBuilder::join(
        GraphBuilder::table("access_edges"),
        reach,
        ["group"],
        ["group"],
    )
    .project_fields([
        ProjectField::renamed("left.resource", "resource"),
        ProjectField::renamed("right.seed", "seed"),
    ]);
    GraphBuilder::join(
        GraphBuilder::table("resources"),
        visible_access,
        ["id"],
        ["resource"],
    )
    .project_fields([
        ProjectField::renamed("left.id", "id"),
        ProjectField::renamed("left.payload", "payload"),
        ProjectField::renamed("right.seed", "seed"),
    ])
}

fn prepare_grant_shape(database: &mut Database<MemoryStorage>) -> crate::ivm::PreparedShape {
    database
        .prepare_one_sink(
            grant_shape_graph(),
            "grant-claim",
            RecordDescriptor::new([("seed", ColumnType::U64.value_type())]),
            ["seed"],
        )
        .unwrap()
}

fn insert_group_edge(batch: &mut DatabaseBatch, id: u64, src: u64, dst: u64) {
    batch.insert(
        "group_edges",
        vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
    );
}

fn insert_access_edge(batch: &mut DatabaseBatch, id: u64, resource: u64, group: u64) {
    batch.insert(
        "access_edges",
        vec![Value::U64(id), Value::U64(resource), Value::U64(group)],
    );
}

fn insert_resource(batch: &mut DatabaseBatch, id: u64, payload: u64) {
    batch.insert("resources", vec![Value::U64(id), Value::U64(payload)]);
}

fn update_edge(batch: &mut DatabaseBatch, id: u64, src: u64, dst: u64) {
    batch.update(
        "edges",
        vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
    );
}

fn sort_pairs_by_value(values: &mut [(Vec<Value>, i64)]) {
    values.sort_by_key(|(values, _)| {
        let Value::U64(src) = &values[0] else {
            unreachable!()
        };
        let Value::U64(dst) = &values[1] else {
            unreachable!()
        };
        (*src, *dst)
    });
}

fn prepared_reachability_oracle(
    seed: u64,
    edges: &[(u64, u64)],
) -> std::collections::BTreeSet<u64> {
    let mut reachable = std::collections::BTreeSet::from([seed]);
    loop {
        let before = reachable.len();
        for (src, dst) in edges {
            if reachable.contains(src) {
                reachable.insert(*dst);
            }
        }
        if reachable.len() == before {
            return reachable;
        }
    }
}

fn seeded_positive_edge_insertions() -> Vec<(u64, u64)> {
    let mut edges = vec![
        (1, 2),
        (2, 3),
        (1, 3),
        (3, 4),
        (4, 2),
        (2, 5),
        (5, 6),
        (3, 6),
        (6, 6),
        (6, 7),
        (7, 3),
        (8, 9),
        (7, 8),
        (8, 1),
        (5, 7),
        (1, 2),
    ];
    let mut state = 0x5eed_cafe_u64;
    for _ in 0..48 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let src = 1 + ((state >> 32) % 9);
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let dst = 1 + ((state >> 32) % 9);
        edges.push((src, dst));
    }
    edges
}

#[test]
fn commits_insert_update_and_delete_batches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    assert!(batch.is_empty());
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        database
            .storage
            .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
            .unwrap(),
        Some(
            database
                .ivm_runtime
                .schema()
                .table("albums")
                .unwrap()
                .record_schema()
                .create(&[Value::U64(7), Value::String("Blue Train".to_owned())])
                .unwrap()
        )
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    let stored = database
        .storage
        .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
        .unwrap()
        .unwrap();
    let descriptor = database
        .ivm_runtime
        .schema()
        .table("albums")
        .unwrap()
        .record_schema();
    assert_eq!(
        descriptor.get(&stored, "title").unwrap(),
        Value::String("Giant Steps".to_owned())
    );

    let mut batch = database.open_batch();
    batch.delete("albums", PrimaryKeyValue::U64(7));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        database
            .storage
            .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
            .unwrap(),
        None
    );
}

#[test]
fn staged_batch_reads_observe_uncommitted_writes() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();

    let mut staged = database.open_staged_batch();
    staged.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    assert_eq!(
        staged
            .primary_key_scan("albums", &[Value::U64(7)])
            .unwrap()
            .into_iter()
            .map(|record| record.get("title").unwrap())
            .collect::<Vec<_>>(),
        vec![Value::String("Blue Train".to_owned())]
    );
    staged.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    assert_eq!(
        staged
            .primary_key_scan("albums", &[Value::U64(7)])
            .unwrap()
            .into_iter()
            .map(|record| record.get("title").unwrap())
            .collect::<Vec<_>>(),
        vec![Value::String("Giant Steps".to_owned())]
    );
    staged.delete("albums", PrimaryKeyValue::U64(7));
    assert!(
        staged
            .primary_key_scan("albums", &[Value::U64(7)])
            .unwrap()
            .is_empty()
    );
    staged.commit().unwrap();

    assert!(
        database
            .primary_key_scan("albums", &[Value::U64(7)])
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .table_delta_records,
        0
    );
}

fn vec_derived_primary_key_scan_raw(
    database: &Database<MemoryStorage>,
    batch: &DatabaseBatch,
    table: &str,
    prefix: &[Value],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut key_prefix = Vec::new();
    for value in prefix {
        encode_primary_key_part(&mut key_prefix, value);
    }
    let mut rows = database
        .primary_key_scan_raw(table, prefix)
        .unwrap()
        .into_iter()
        .map(EncodedKeyValue::into_parts)
        .collect::<std::collections::BTreeMap<_, _>>();
    for write in database
        .pending_writes_from_operations(&batch.operations)
        .unwrap()
    {
        if write.table() != table || !write.key().starts_with(&key_prefix) {
            continue;
        }
        match write {
            PendingTableWrite::Set { key, record, .. } => {
                rows.insert(key, record);
            }
            PendingTableWrite::Delete { key, .. } => {
                rows.remove(&key);
            }
        }
    }
    rows.into_iter().collect()
}

#[test]
fn staged_batch_storage_txn_handles_large_accumulated_batches() {
    let database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let mut batch = database.open_batch();
    for id in 0..10_000 {
        batch.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("album-{id}"))],
        );
    }

    let rows = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(9_999)])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].record().get("title").unwrap(),
        Value::String("album-9999".to_owned())
    );
    assert_eq!(batch.txn_operations.borrow().len(), 10_000);
    assert_eq!(
        rows.iter()
            .cloned()
            .map(EncodedKeyValue::into_parts)
            .collect::<Vec<_>>(),
        vec_derived_primary_key_scan_raw(&database, &batch, "albums", &[Value::U64(9_999)])
    );

    let cached_rows = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(42)])
        .unwrap();
    assert_eq!(
        cached_rows[0].record().get("title").unwrap(),
        Value::String("album-42".to_owned())
    );

    batch.update(
        "albums",
        vec![Value::U64(42), Value::String("updated".to_owned())],
    );
    let updated = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(42)])
        .unwrap();
    assert_eq!(
        updated[0].record().get("title").unwrap(),
        Value::String("updated".to_owned())
    );
    assert_eq!(
        updated
            .iter()
            .cloned()
            .map(EncodedKeyValue::into_parts)
            .collect::<Vec<_>>(),
        vec_derived_primary_key_scan_raw(&database, &batch, "albums", &[Value::U64(42)])
    );

    batch.delete("albums", PrimaryKeyValue::U64(42));
    assert!(
        database
            .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(42)])
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        database
            .primary_key_scan_raw_in_batch(&batch, "albums", &[])
            .unwrap()
            .len(),
        9_999
    );
    assert_eq!(batch.txn_indexed_operations.get(), batch.operations.len());
}

#[test]
fn primary_key_get_raw_observes_staged_overlay() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "albums",
        vec![Value::U64(1), Value::String("stored-one".to_owned())],
    );
    seed.insert(
        "albums",
        vec![Value::U64(2), Value::String("stored-two".to_owned())],
    );
    database.commit_batch(seed).unwrap();

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(1), Value::String("updated-one".to_owned())],
    );
    batch.delete("albums", PrimaryKeyValue::U64(2));
    batch.insert(
        "albums",
        vec![Value::U64(3), Value::String("inserted-three".to_owned())],
    );

    let updated = database
        .primary_key_get_raw_in_batch(&batch, "albums", &[Value::U64(1)])
        .unwrap()
        .unwrap();
    assert_eq!(
        updated.record().get("title").unwrap(),
        Value::String("updated-one".to_owned())
    );
    assert!(
        database
            .primary_key_get_raw_in_batch(&batch, "albums", &[Value::U64(2)])
            .unwrap()
            .is_none()
    );
    let inserted = database
        .primary_key_get_raw_in_batch(&batch, "albums", &[Value::U64(3)])
        .unwrap()
        .unwrap();
    assert_eq!(
        inserted.record().get("title").unwrap(),
        Value::String("inserted-three".to_owned())
    );
    assert_eq!(batch.txn_indexed_operations.get(), batch.operations.len());
}

#[test]
fn staged_batch_storage_txn_overlays_storage_for_prefix_scans() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "albums",
        vec![Value::U64(1), Value::String("stored-one".to_owned())],
    );
    seed.insert(
        "albums",
        vec![Value::U64(2), Value::String("stored-two".to_owned())],
    );
    database.commit_batch(seed).unwrap();

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(1), Value::String("staged-one".to_owned())],
    );
    batch.delete("albums", PrimaryKeyValue::U64(2));
    batch.insert(
        "albums",
        vec![Value::U64(3), Value::String("staged-three".to_owned())],
    );

    let rows = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[])
        .unwrap()
        .into_iter()
        .map(|row| row.record().get("title").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            Value::String("staged-one".to_owned()),
            Value::String("staged-three".to_owned())
        ]
    );
    assert_eq!(
        database
            .primary_key_scan_raw_in_batch(&batch, "albums", &[])
            .unwrap()
            .into_iter()
            .map(EncodedKeyValue::into_parts)
            .collect::<Vec<_>>(),
        vec_derived_primary_key_scan_raw(&database, &batch, "albums", &[])
    );
}

#[test]
fn staged_batch_storage_txn_advances_only_new_operations() {
    let database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let mut batch = database.open_batch();
    for id in 0..10_000 {
        batch.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("album-{id}"))],
        );
    }
    database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(9_999)])
        .unwrap();
    assert_eq!(batch.txn_indexed_operations.get(), 10_000);

    for id in 10_000..20_000 {
        batch.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("album-{id}"))],
        );
    }
    database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(19_999)])
        .unwrap();
    assert_eq!(batch.txn_indexed_operations.get(), 20_000);
    assert_eq!(batch.txn_operations.borrow().len(), 20_000);

    batch.update(
        "albums",
        vec![Value::U64(19_999), Value::String("tail-updated".to_owned())],
    );
    database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(19_999)])
        .unwrap();
    assert_eq!(batch.txn_indexed_operations.get(), 20_001);
    assert_eq!(batch.txn_operations.borrow().len(), 20_001);
}

#[test]
fn staged_batch_commit_ticks_once_for_multiple_writes() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut staged = database.open_staged_batch();
    staged.insert(
        "albums",
        vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
    );
    staged.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".to_owned())],
    );
    staged.commit().unwrap();

    let metrics = database.last_commit_metrics().unwrap();
    assert_eq!(metrics.tick.table_delta_records, 2);
    assert_eq!(metrics.tick.notifications_sent, 1);
    assert_eq!(metrics.tick.notification_records, 2);
    let mut observed = subscription.recv().unwrap().to_values().unwrap();
    observed.sort_by_key(|(values, _)| match values[0] {
        Value::U64(id) => id,
        _ => panic!("expected u64 id"),
    });
    assert_eq!(
        observed,
        vec![
            (
                vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
                1
            ),
            (
                vec![Value::U64(2), Value::String("Blue Train".to_owned())],
                1
            ),
        ]
    );
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn staged_batch_commit_matches_one_shot_wrapper() {
    let mut staged_db = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let mut wrapper_db = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();

    let mut staged = staged_db.open_staged_batch();
    staged.insert(
        "albums",
        vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
    );
    staged.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".to_owned())],
    );
    staged.delete("albums", PrimaryKeyValue::U64(1));
    staged.commit().unwrap();

    let mut wrapper = wrapper_db.open_batch();
    wrapper.insert(
        "albums",
        vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
    );
    wrapper.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".to_owned())],
    );
    wrapper.delete("albums", PrimaryKeyValue::U64(1));
    wrapper_db.commit_batch(wrapper).unwrap();

    assert_eq!(
        staged_db
            .primary_key_scan("albums", &[])
            .unwrap()
            .into_iter()
            .map(|record| record.to_values())
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        wrapper_db
            .primary_key_scan("albums", &[])
            .unwrap()
            .into_iter()
            .map(|record| record.to_values())
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    );
    assert_eq!(
        staged_db
            .last_commit_metrics()
            .unwrap()
            .tick
            .table_delta_records,
        wrapper_db
            .last_commit_metrics()
            .unwrap()
            .tick
            .table_delta_records
    );
    assert_eq!(
        staged_db.last_commit_metrics().unwrap().storage_writes,
        wrapper_db.last_commit_metrics().unwrap().storage_writes
    );
}

#[test]
fn direct_record_store_stores_ordered_records_independent_of_tables() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = albums_schema().with_direct_record_store(DirectRecordStoreSchema::new(
        "streams",
        RecordDescriptor::new([
            ("namespace", ColumnType::String.value_type()),
            ("path", ColumnType::String.value_type()),
        ]),
        RecordDescriptor::new([("bytes", ColumnType::Bytes.value_type())]),
    ));
    let column_families = schema.column_families();
    let storage = RocksDbStorage::open(temp_dir.path(), &column_families).unwrap();
    let mut database = Database::new(schema.clone(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    {
        let store = database.direct_record_store("streams").unwrap();
        store
            .set(
                &[
                    Value::String("content".to_owned()),
                    Value::String("content/02".to_owned()),
                ],
                &[Value::Bytes(b"two".to_vec())],
            )
            .unwrap();
        store
            .set(
                &[
                    Value::String("content".to_owned()),
                    Value::String("content/01".to_owned()),
                ],
                &[Value::Bytes(b"one".to_vec())],
            )
            .unwrap();
        store
            .set(
                &[
                    Value::String("content".to_owned()),
                    Value::String("content/03".to_owned()),
                ],
                &[Value::Bytes(b"three".to_vec())],
            )
            .unwrap();
        store
            .set(
                &[
                    Value::String("checkpoint".to_owned()),
                    Value::String("checkpoint".to_owned()),
                ],
                &[Value::Bytes(b"cp".to_vec())],
            )
            .unwrap();

        assert_eq!(
            store
                .get(&[
                    Value::String("content".to_owned()),
                    Value::String("content/02".to_owned()),
                ])
                .unwrap()
                .unwrap()
                .get("bytes")
                .unwrap(),
            Value::Bytes(b"two".to_vec())
        );
        assert_eq!(
            store
                .range(
                    &[
                        Value::String("content".to_owned()),
                        Value::String("content/01".to_owned()),
                    ],
                    &[
                        Value::String("content".to_owned()),
                        Value::String("content/04".to_owned()),
                    ]
                )
                .unwrap()
                .into_iter()
                .map(|record| record.get("bytes").unwrap())
                .collect::<Vec<_>>(),
            vec![
                Value::Bytes(b"one".to_vec()),
                Value::Bytes(b"two".to_vec()),
                Value::Bytes(b"three".to_vec()),
            ],
        );
        assert_eq!(
            store
                .prefix(&[Value::String("content".to_owned())])
                .unwrap()
                .into_iter()
                .map(|record| record.get("bytes").unwrap())
                .collect::<Vec<_>>(),
            vec![
                Value::Bytes(b"one".to_vec()),
                Value::Bytes(b"two".to_vec()),
                Value::Bytes(b"three".to_vec()),
            ],
        );

        let raw_value = database
            .storage
            .get(
                "streams",
                &PrimaryKeyValue::Composite(vec![
                    PrimaryKeyValue::String("content".to_owned()),
                    PrimaryKeyValue::String("content/01".to_owned()),
                ])
                .into_bytes(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(raw_value, b"one");

        store
            .delete(&[
                Value::String("content".to_owned()),
                Value::String("content/02".to_owned()),
            ])
            .unwrap();
        assert!(
            store
                .get(&[
                    Value::String("content".to_owned()),
                    Value::String("content/02".to_owned()),
                ])
                .unwrap()
                .is_none()
        );
    }
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
    assert!(database.primary_key_scan("albums", &[]).unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        vec![(
            vec![Value::U64(7), Value::String("Blue Train".to_owned())],
            1
        )]
    );
    assert_eq!(
        database
            .direct_record_store("streams")
            .unwrap()
            .get(&[
                Value::String("checkpoint".to_owned()),
                Value::String("checkpoint".to_owned()),
            ])
            .unwrap()
            .unwrap()
            .get("bytes")
            .unwrap(),
        Value::Bytes(b"cp".to_vec())
    );
    assert_eq!(database.storage.get("albums", b"content/01").unwrap(), None);

    drop(database);
    let column_families = schema.column_families();
    let storage = RocksDbStorage::open(temp_dir.path(), &column_families).unwrap();
    let reopened = Database::new(schema, storage).unwrap();
    let store = reopened.direct_record_store("streams").unwrap();
    assert_eq!(
        store
            .prefix(&[Value::String("content".to_owned())])
            .unwrap()
            .into_iter()
            .map(|record| record.get("bytes").unwrap())
            .collect::<Vec<_>>(),
        vec![
            Value::Bytes(b"one".to_vec()),
            Value::Bytes(b"three".to_vec()),
        ],
    );
    assert_eq!(
        reopened
            .primary_key_scan("albums", &[Value::U64(7)])
            .unwrap()
            .into_iter()
            .map(|record| record.get("title").unwrap())
            .collect::<Vec<_>>(),
        vec![Value::String("Blue Train".to_owned())]
    );
}

#[test]
fn commit_metrics_split_storage_and_tick_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    database.set_tick_runtime_stats_enabled(true);
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let metrics = database.last_commit_metrics().unwrap();
    assert_eq!(metrics.storage_write_count, 1);
    assert!(metrics.storage_write_bytes > 0);
    assert_eq!(metrics.tick.table_delta_records, 1);
    assert_eq!(metrics.tick.notifications_sent, 1);
    assert_eq!(metrics.tick.notification_records, 1);
    assert!(metrics.tick.runtime_stats.graph_nodes > 0);
}

#[test]
fn commit_metrics_split_storage_writes_by_jazz_destination() {
    fn run(layout: StorageLayout) -> StorageWriteMetrics {
        let schema = DatabaseSchema::new([
            TableSchema::new(
                "jazz_docs_history",
                [
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("tx_time", ColumnType::U64),
                    ColumnSchema::new("tx_node_id", ColumnType::U64),
                    ColumnSchema::new("parent", ColumnType::Uuid),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
            ]))
            .with_index(IndexSchema::new(
                "by_tx",
                ["tx_time", "tx_node_id", "row_uuid"],
            )),
            TableSchema::new(
                "jazz_docs_global_current",
                [
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("tx_time", ColumnType::U64),
                    ColumnSchema::new("tx_node_id", ColumnType::U64),
                    ColumnSchema::new("user_parent", ColumnType::Uuid),
                ],
            )
            .with_primary_key(PrimaryKey::composite([PrimaryKeyColumn::uuid("row_uuid")]))
            .with_index(IndexSchema::new("by_user_parent", ["user_parent"])),
            TableSchema::new(
                "jazz_docs_register_global_current",
                [
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("tx_time", ColumnType::U64),
                ],
            )
            .with_primary_key(PrimaryKey::composite([PrimaryKeyColumn::uuid("row_uuid")])),
            TableSchema::new(
                "jazz_global_changes",
                [
                    ColumnSchema::new("table_name", ColumnType::Bytes),
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("layer", ColumnType::Bytes),
                    ColumnSchema::new("global_seq", ColumnType::U64),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::bytes("table_name"),
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::bytes("layer"),
                PrimaryKeyColumn::integer("global_seq", IntegerKeyType::U64),
            ]))
            .with_index(IndexSchema::new(
                "by_global_seq",
                ["global_seq", "table_name", "row_uuid", "layer"],
            )),
            TableSchema::new(
                "jazz_transactions",
                [
                    ColumnSchema::new("time", ColumnType::U64),
                    ColumnSchema::new("node_id", ColumnType::U64),
                    ColumnSchema::new("global_seq", ColumnType::U64),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::integer("time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("node_id", IntegerKeyType::U64),
            ]))
            .with_index(IndexSchema::new("by_global_seq", ["global_seq"])),
        ]);
        let column_families = layout.physical_column_families(schema.column_families());
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let storage = MemoryStorage::new(&refs);
        let mut database = Database::new_with_storage_layout(schema, storage, layout).unwrap();
        let row_uuid = uuid(1);

        let mut batch = database.open_batch();
        batch.insert(
            "jazz_docs_history",
            vec![
                Value::Uuid(row_uuid),
                Value::U64(1),
                Value::U64(2),
                Value::Uuid(uuid(3)),
            ],
        );
        batch.insert(
            "jazz_docs_global_current",
            vec![
                Value::Uuid(row_uuid),
                Value::U64(1),
                Value::U64(2),
                Value::Uuid(uuid(3)),
            ],
        );
        batch.insert(
            "jazz_docs_register_global_current",
            vec![Value::Uuid(row_uuid), Value::U64(1)],
        );
        batch.insert(
            "jazz_global_changes",
            vec![
                Value::Bytes(b"docs".to_vec()),
                Value::Uuid(row_uuid),
                Value::Bytes(b"content".to_vec()),
                Value::U64(1),
            ],
        );
        batch.insert(
            "jazz_transactions",
            vec![Value::U64(1), Value::U64(2), Value::U64(1)],
        );
        database.commit_batch(batch).unwrap();

        database.last_commit_metrics().unwrap().storage_writes
    }

    let writes = run(StorageLayout::Identity);
    assert_eq!(writes.total.count, 9);
    assert_eq!(writes.history_rows.count, 1);
    assert_eq!(writes.history_indexes.count, 1);
    assert_eq!(writes.global_current_rows.count, 1);
    assert_eq!(writes.global_current_indexes.count, 1);
    assert_eq!(writes.register_global_current_rows.count, 1);
    assert_eq!(writes.global_changes_rows.count, 1);
    assert_eq!(writes.global_changes_indexes.count, 1);
    assert_eq!(writes.transactions_rows.count, 1);
    assert_eq!(writes.transactions_indexes.count, 1);
    assert_eq!(writes.other.count, 0);

    let class_writes = run(StorageLayout::jazz_class_v1());
    assert_eq!(class_writes, writes);
}

#[test]
fn subscribe_sends_empty_hydration_snapshot_without_writes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();

    assert!(subscription_id.try_recv().unwrap().is_empty());
    database.flush().unwrap();
    assert!(subscription_id.try_recv().is_err());
    assert!(database.storage.prefix("albums", b"").unwrap().is_empty());
}

#[test]
fn history_windows_are_transparent_to_subscription_hydration() {
    let mut database = jazz_docs_history_database();
    let row_count = 260;
    seed_jazz_docs_history(&mut database, 0, row_count);

    database
        .consolidate_table_windows("jazz_docs_history", TARGET_RECORDS_PER_WINDOW)
        .unwrap();
    assert!(
        database
            .storage
            .prefix("jazz_docs_history", b"")
            .unwrap()
            .len()
            < row_count as usize
    );

    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("jazz_docs_history"))
        .unwrap();
    let rows = subscription.recv().unwrap();
    assert_eq!(rows.deltas.len(), row_count as usize);

    let indexed = database
        .index_scan_raw(
            "jazz_docs_history",
            "by_tx",
            &[Value::U64(128), Value::U64(7)],
        )
        .unwrap();
    assert_eq!(indexed.len(), 1);
}

#[test]
fn post_tick_history_consolidation_preserves_live_subscription_deltas_and_hydration() {
    let mut control = jazz_docs_history_database();
    let mut consolidated = jazz_docs_history_database();
    seed_jazz_docs_history(&mut control, 0, 260);
    seed_jazz_docs_history(&mut consolidated, 0, 260);

    let control_live = control
        .subscribe_one_sink(GraphBuilder::table("jazz_docs_history"))
        .unwrap();
    let consolidated_live = consolidated
        .subscribe_one_sink(GraphBuilder::table("jazz_docs_history"))
        .unwrap();
    assert_eq!(
        control_live.recv().unwrap(),
        consolidated_live.recv().unwrap()
    );
    control.flush().unwrap();
    consolidated.flush().unwrap();

    let report = consolidated
        .consolidate_history_windows(TARGET_RECORDS_PER_WINDOW, 2)
        .unwrap();
    assert_eq!(report.windows, 1);
    assert_eq!(report.records, TARGET_RECORDS_PER_WINDOW);

    seed_jazz_docs_history(&mut control, 260, 1);
    seed_jazz_docs_history(&mut consolidated, 260, 1);
    assert_eq!(
        control_live.recv().unwrap(),
        consolidated_live.recv().unwrap()
    );

    let control_fresh = control
        .subscribe_one_sink(GraphBuilder::table("jazz_docs_history"))
        .unwrap();
    let consolidated_fresh = consolidated
        .subscribe_one_sink(GraphBuilder::table("jazz_docs_history"))
        .unwrap();
    assert_eq!(
        control_fresh.recv().unwrap(),
        consolidated_fresh.recv().unwrap()
    );
}

#[test]
fn history_consolidation_visits_direct_record_stores() {
    let schema = DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
        "jazz_docs_history",
        RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("tx_time", ValueType::U64),
            ("tx_node_id", ValueType::Uuid),
        ]),
        RecordDescriptor::new([("body", ValueType::Bytes)]),
    ));
    let storage = MemoryStorage::new(&schema.column_families());
    let database = Database::new(schema, storage).unwrap();
    let store = database.direct_record_store("jazz_docs_history").unwrap();
    let row = uuid::Uuid::from_u128(7);
    let node = uuid::Uuid::from_u128(9);
    for idx in 0..(TARGET_RECORDS_PER_WINDOW + 3) {
        store
            .set(
                &[Value::Uuid(row), Value::U64(idx as u64), Value::Uuid(node)],
                &[Value::Bytes(vec![idx as u8])],
            )
            .unwrap();
    }

    let report = database
        .consolidate_history_windows(TARGET_RECORDS_PER_WINDOW, 2)
        .unwrap();

    assert_eq!(
        report,
        WindowConsolidation {
            windows: 1,
            records: TARGET_RECORDS_PER_WINDOW
        }
    );
    assert_eq!(
        store
            .get(&[
                Value::Uuid(row),
                Value::U64((TARGET_RECORDS_PER_WINDOW + 2) as u64),
                Value::Uuid(node),
            ])
            .unwrap()
            .unwrap()
            .get("body")
            .unwrap(),
        Value::Bytes(vec![(TARGET_RECORDS_PER_WINDOW + 2) as u8])
    );
}

#[test]
fn partial_history_tail_is_marked_converged_until_dirtied() {
    let schema = jazz_docs_history_schema();
    let layout = StorageLayout::jazz_class_v1();
    let physical_cfs = layout.physical_column_families(schema.column_families());
    let physical_cf_refs = physical_cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = ScanCountingStorage::new(&physical_cf_refs);
    let counter = storage.clone();
    let mut database = Database::new_with_storage_layout(schema, storage, layout).unwrap();

    seed_jazz_docs_history(&mut database, 0, 3);
    let scans_after_seed = counter.scan_range_count();

    let first = database.consolidate_history_windows(4, 2).unwrap();
    assert_eq!(first, WindowConsolidation::default());
    assert!(counter.scan_range_count() > scans_after_seed);

    let scans_after_first = counter.scan_range_count();
    let second = database.consolidate_history_windows(4, 2).unwrap();
    assert_eq!(second, WindowConsolidation::default());
    assert_eq!(counter.scan_range_count(), scans_after_first);

    seed_jazz_docs_history(&mut database, 3, 1);
    let scans_after_dirty = counter.scan_range_count();
    let after_dirty = database.consolidate_history_windows(4, 2).unwrap();
    assert_eq!(
        after_dirty,
        WindowConsolidation {
            windows: 1,
            records: 4
        }
    );
    assert!(counter.scan_range_count() > scans_after_dirty);

    let scans_after_reconverge = counter.scan_range_count();
    let after_reconverge = database.consolidate_history_windows(4, 2).unwrap();
    assert_eq!(after_reconverge, WindowConsolidation::default());
    assert!(counter.scan_range_count() > scans_after_reconverge);

    let scans_after_tail_converged = counter.scan_range_count();
    let skipped = database.consolidate_history_windows(4, 2).unwrap();
    assert_eq!(skipped, WindowConsolidation::default());
    assert_eq!(counter.scan_range_count(), scans_after_tail_converged);
}

fn jazz_docs_history_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "jazz_docs_history",
        [
            ColumnSchema::new("row_uuid", ColumnType::Uuid),
            ColumnSchema::new("tx_time", ColumnType::U64),
            ColumnSchema::new("tx_node", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::uuid("row_uuid"),
        PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("tx_node", IntegerKeyType::U64),
    ]))
    .with_index(IndexSchema::new(
        "by_tx",
        ["tx_time", "tx_node", "row_uuid"],
    ))])
}

fn jazz_docs_history_database() -> Database<MemoryStorage> {
    let schema = jazz_docs_history_schema();
    let layout = StorageLayout::jazz_class_v1();
    let physical_cfs = layout.physical_column_families(schema.column_families());
    let physical_cf_refs = physical_cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = MemoryStorage::new(&physical_cf_refs);
    Database::new_with_storage_layout(schema, storage, layout).unwrap()
}

#[derive(Clone)]
struct ScanCountingStorage {
    inner: MemoryStorage,
    scan_range_count: Rc<Cell<usize>>,
}

impl ScanCountingStorage {
    fn new(column_families: &[&str]) -> Self {
        Self {
            inner: MemoryStorage::new(column_families),
            scan_range_count: Rc::new(Cell::new(0)),
        }
    }

    fn scan_range_count(&self) -> usize {
        self.scan_range_count.get()
    }
}

impl OrderedKvStorage for ScanCountingStorage {
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<StorageValue>, StorageError> {
        self.inner.get(cf, key)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), StorageError> {
        self.inner.set(cf, key, value)
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), StorageError> {
        self.inner.delete(cf, key)
    }

    fn close(&self) -> Result<(), StorageError> {
        self.inner.close()
    }

    fn approximate_class_bytes(&self, cf: &ColumnFamilyName) -> Result<Option<u64>, StorageError> {
        self.inner.approximate_class_bytes(cf)
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        self.scan_range_count
            .set(self.scan_range_count.get().saturating_add(1));
        self.inner.scan_range(cf, start, end, visit)
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        self.inner.scan_prefix(cf, prefix, visit)
    }

    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        self.inner.scan_prefix_reverse(cf, prefix, visit)
    }

    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<KeyValue>, StorageError> {
        self.inner.last_with_prefix(cf, prefix)
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, StorageError> {
        self.inner.last_with_prefix_before_or_at(cf, prefix, upper)
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), StorageError> {
        self.inner.write_many(operations)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }
}

fn seed_jazz_docs_history<S: OrderedKvStorage>(
    database: &mut Database<S>,
    start_idx: u64,
    row_count: u64,
) {
    let mut batch = database.open_batch();
    for idx in start_idx..start_idx + row_count {
        batch.insert(
            "jazz_docs_history",
            vec![
                Value::Uuid(uuid::Uuid::from_u128(
                    0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa,
                )),
                Value::U64(100 + idx),
                Value::U64(7),
                Value::String(format!("payload-{idx}")),
            ],
        );
    }
    database.commit_batch(batch).unwrap();
}

#[test]
fn rejects_unknown_tables() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("missing", vec![Value::U64(1)]);

    assert!(matches!(
        database.commit_batch(batch).unwrap_err(),
        Error::TableNotFound(table) if table == "missing"
    ));
}

#[test]
fn invalid_batches_do_not_partially_write_valid_earlier_operations() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.insert("missing", vec![Value::U64(1)]);

    assert!(matches!(
        database.commit_batch(batch),
        Err(Error::TableNotFound(table)) if table == "missing"
    ));
    assert!(database.storage.prefix("albums", b"").unwrap().is_empty());
}

#[test]
fn final_atomic_commit_failure_leaves_base_rows_unwritten_and_poisons_database() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );

    assert!(matches!(
        database.commit_batch(batch),
        Err(Error::Storage(error)) if matches!(
            error.as_ref(),
            crate::storage::Error::ColumnFamilyNotFound(cf) if cf == "indices"
        )
    ));
    assert_eq!(
        database
            .storage
            .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
            .unwrap(),
        None
    );
    assert!(matches!(
        database.primary_key_scan("albums", &[]),
        Err(Error::DatabasePoisoned)
    ));
}

#[test]
fn atomic_commit_path_supports_indexed_join_and_recursive_workloads() {
    let indexed_storage = MemoryStorage::new(&["albums", "indices"]);
    let mut indexed = Database::new(indexed_albums_schema(), indexed_storage).unwrap();
    let mut batch = indexed.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    indexed.commit_batch(batch).unwrap();
    assert_eq!(
        record_values(
            indexed
                .index_scan(
                    "albums",
                    "albums_by_title",
                    &[Value::String("Blue Train".to_owned())],
                )
                .unwrap()
        ),
        [vec![Value::U64(7), Value::String("Blue Train".to_owned())]]
    );

    let join_storage = MemoryStorage::new(&["albums", "artists"]);
    let mut joined = Database::new(albums_artists_schema(), join_storage).unwrap();
    let subscription = joined
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();
    let mut batch = joined.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    joined.commit_batch(batch).unwrap();
    assert_eq!(expect_recv_vals(&subscription).len(), 1);

    let recursive_storage = MemoryStorage::new(&["edges"]);
    let mut recursive = Database::new(edges_schema(), recursive_storage).unwrap();
    let subscription = recursive
        .subscribe_one_sink(reachability_graph(16))
        .unwrap();
    let mut batch = recursive.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(1), Value::U64(2)]);
    batch.insert("edges", vec![Value::U64(2), Value::U64(2), Value::U64(3)]);
    recursive.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        vec![
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
            (vec![Value::U64(2), Value::U64(3)], 1),
        ]
    );
}

#[test]
fn subscriptions_reject_unknown_tables_and_indices() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();

    assert!(matches!(
        database.subscribe_one_sink(GraphBuilder::table("missing")),
        Err(Error::IvmRuntime(IvmRuntimeError::TableNotFound(table))) if table == "missing"
    ));
    assert!(matches!(
        database.subscribe_one_sink(GraphBuilder::index("albums", "missing_idx")),
        Err(Error::IvmRuntime(IvmRuntimeError::IndexNotFound(index))) if index == "missing_idx"
    ));
}

#[test]
fn rejects_primary_key_type_mismatches_before_writing() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::String),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(schema, storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::String("not-a-u64".to_owned()),
            Value::String("Blue Train".to_owned()),
        ],
    );

    assert!(matches!(
        database.commit_batch(batch),
        Err(Error::PrimaryKeyTypeMismatch { table, column })
            if table == "albums" && column == "id"
    ));
    assert!(database.storage.prefix("albums", b"").unwrap().is_empty());
}

#[test]
fn inserts_accept_values_in_table_declaration_order_even_when_storage_order_differs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let schema = DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("rating", ColumnType::F64.nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let mut database = Database::new(schema, storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
            Value::Nullable(Some(Box::new(Value::F64(4.5)))),
        ],
    );
    database.commit_batch(batch).unwrap();

    let descriptor = database
        .ivm_runtime
        .schema()
        .table("albums")
        .unwrap()
        .record_schema();
    let stored = database
        .storage
        .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
        .unwrap()
        .unwrap();

    assert_eq!(descriptor.get(&stored, "id").unwrap(), Value::U64(7));
    assert_eq!(
        descriptor.get(&stored, "title").unwrap(),
        Value::String("Blue Train".to_owned())
    );
    assert_eq!(
        descriptor.get(&stored, "rating").unwrap(),
        Value::Nullable(Some(Box::new(Value::F64(4.5))))
    );
}

#[test]
fn integer_primary_keys_are_stored_with_tagged_order_preserving_keys() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(
        temp_dir.path(),
        &["u8_keys", "u16_keys", "u32_keys", "u64_keys"],
    )
    .unwrap();
    let mut database = Database::new(integer_key_widths_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("u8_keys", vec![Value::U8(7)]);
    batch.insert("u16_keys", vec![Value::U16(0x0102)]);
    batch.insert("u32_keys", vec![Value::U32(0x0102_0304)]);
    batch.insert("u64_keys", vec![Value::U64(0x0102_0304_0506_0708)]);

    database.commit_batch(batch).unwrap();

    assert!(
        database
            .storage
            .get("u8_keys", &[0x00, 0x07])
            .unwrap()
            .is_some()
    );
    assert!(
        database
            .storage
            .get("u16_keys", &[0x01, 0x01, 0x02])
            .unwrap()
            .is_some()
    );
    assert!(
        database
            .storage
            .get("u32_keys", &[0x02, 0x01, 0x02, 0x03, 0x04])
            .unwrap()
            .is_some()
    );
    assert!(
        database
            .storage
            .get(
                "u64_keys",
                &[0x03, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
            )
            .unwrap()
            .is_some()
    );
}

#[test]
fn composite_primary_keys_are_encoded_from_multiple_columns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history"]).unwrap();
    let mut database = Database::new(composite_key_schema(), storage).unwrap();
    let row_uuid = vec![1, 0, 2];
    let key = PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::Bytes(row_uuid.clone()),
        PrimaryKeyValue::U64(9),
        PrimaryKeyValue::U64(42),
    ])
    .into_bytes();

    let mut batch = database.open_batch();
    batch.insert(
        "history",
        vec![
            Value::Bytes(row_uuid),
            Value::U64(9),
            Value::U64(42),
            Value::String("first".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let descriptor = database
        .ivm_runtime
        .schema()
        .table("history")
        .unwrap()
        .record_schema();
    let stored = database.storage.get("history", &key).unwrap().unwrap();
    assert_eq!(
        descriptor.get(&stored, "payload").unwrap(),
        Value::String("first".to_owned())
    );

    let mut batch = database.open_batch();
    batch.delete(
        "history",
        PrimaryKeyValue::Composite(vec![
            PrimaryKeyValue::Bytes(vec![1, 0, 2]),
            PrimaryKeyValue::U64(9),
            PrimaryKeyValue::U64(42),
        ]),
    );
    database.commit_batch(batch).unwrap();

    assert!(database.storage.get("history", &key).unwrap().is_none());
}

#[test]
fn rejects_tables_without_primary_keys() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["logs"]).unwrap();
    let mut database = Database::new(
        DatabaseSchema::new([TableSchema::new(
            "logs",
            [ColumnSchema::new("message", ColumnType::String)],
        )]),
        storage,
    )
    .unwrap();
    let mut batch = database.open_batch();
    batch.insert("logs", vec![Value::String("hello".to_owned())]);

    assert!(matches!(
        database.commit_batch(batch).unwrap_err(),
        Error::MissingPrimaryKey(table) if table == "logs"
    ));
}

#[test]
fn table_subscriptions_receive_insert_update_and_delete_messages() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec![7_u64.into(), "Blue Train".into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription_id),
        [
            (vec![7_u64.into(), "Blue Train".into()], -1),
            (vec![7_u64.into(), "Giant Steps".into()], 1)
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("albums", PrimaryKeyValue::U64(7));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec![7_u64.into(), "Giant Steps".into()], -1)]
    );
}

#[test]
fn dropping_subscription_receiver_unsubscribes_on_next_message() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    let subscription_id = subscription.id();
    drop(subscription);

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert!(!database.unsubscribe(subscription_id));
}

#[test]
fn subscribe_returns_current_rows_as_initial_message_then_future_deltas() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    database.flush().unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), "Blue Train".into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![8_u64.into(), "Giant Steps".into()], 1)]
    );
}

#[test]
fn subscribe_query_filters_current_rows_in_initial_message() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Too Early".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_query(select_query(
            Select::new([SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    col("id"),
                    BinaryOp::Gt,
                    Expr::Literal(Value::U64(10)),
                )),
        ))
        .unwrap();

    database.flush().unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[test]
fn subscription_reports_incremental_query_deltas_through_database_facade() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription = database
        .subscribe_query(select_query(
            Select::new([SelectItem::expr(col("id")), SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    col("id"),
                    BinaryOp::Gt,
                    Expr::Literal(Value::U64(10)),
                )),
        ))
        .unwrap();

    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(5), Value::String("Out of Scope".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(13), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train".into()], 1),
            (vec![13_u64.into(), "Giant Steps".into()], 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![
            Value::U64(5),
            Value::String("Still Out of Scope".to_owned()),
        ],
    );
    batch.update(
        "albums",
        vec![
            Value::U64(11),
            Value::String("Blue Train Take Two".to_owned()),
        ],
    );
    batch.delete("albums", PrimaryKeyValue::U64(13));
    database.commit_batch(batch).unwrap();

    // Subscription messages expose weighted result deltas, not full snapshots:
    // unchanged matching rows are absent, the updated row is retracted and
    // re-added, and base-table changes outside the query are not reported.
    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train Take Two".into()], 1),
            (vec![11_u64.into(), "Blue Train".into()], -1),
            (vec![13_u64.into(), "Giant Steps".into()], -1),
        ]
    );
}

#[test]
fn subscription_reports_incremental_contains_filter_deltas() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(
            GraphBuilder::table("albums")
                .filter(PredicateExpr::Contains {
                    field: "title".to_owned(),
                    value: Value::String("Train".to_owned()).into(),
                })
                .project_fields([ProjectField::named("id"), ProjectField::named("title")]),
        )
        .unwrap();

    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Out of Scope".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![11_u64.into(), "Blue Train".into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(11), Value::String("Blue Seven".to_owned())],
    );
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Night Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train".into()], -1),
            (vec![7_u64.into(), "Night Train".into()], 1),
        ]
    );
}

#[test]
fn prepared_subscription_reports_incremental_eq_field_filter_deltas() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.value_type())]);
    let routing_field = "__routing";
    let binding = GraphBuilder::binding_source("title_eq_param", binding_descriptor)
        .project_fields([
            ProjectField::named("wanted"),
            ProjectField::literal(routing_field, Value::U8(0)),
        ]);
    let albums = GraphBuilder::table("albums").project_fields([
        ProjectField::named("id"),
        ProjectField::named("title"),
        ProjectField::literal(routing_field, Value::U8(0)),
    ]);
    let graph = GraphBuilder::join(binding, albums, [routing_field], [routing_field])
        .project_fields([
            ProjectField::renamed("right.id", "id"),
            ProjectField::renamed("right.title", "title"),
            ProjectField::renamed("left.wanted", "wanted"),
        ])
        .filter(PredicateExpr::EqField {
            field: "title".to_owned(),
            value_field: "wanted".to_owned(),
        });
    let shape = database
        .prepare_one_sink(graph, "title_eq_param", binding_descriptor, ["wanted"])
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
        .unwrap();

    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Out of Scope".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![
                11_u64.into(),
                "Blue Train".into(),
                Value::String("Blue Train".to_owned()),
            ],
            1,
        )]
    );
}

#[test]
fn prepared_binding_source_reuse_validates_descriptor() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let string_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.value_type())]);
    let string_graph = GraphBuilder::binding_source("shared_params", string_descriptor)
        .project_fields([ProjectField::named("wanted")]);

    database
        .prepare_one_sink(
            string_graph.clone(),
            "shared_params",
            string_descriptor,
            ["wanted"],
        )
        .unwrap();
    database
        .prepare_one_sink(string_graph, "shared_params", string_descriptor, ["wanted"])
        .unwrap();

    let u64_descriptor = RecordDescriptor::new([("wanted", ColumnType::U64.value_type())]);
    let u64_graph = GraphBuilder::binding_source("shared_params", u64_descriptor)
        .project_fields([ProjectField::named("wanted")]);
    let err = database
        .prepare_one_sink(u64_graph, "shared_params", u64_descriptor, ["wanted"])
        .unwrap_err();
    assert!(matches!(
        err,
        Error::IvmRuntime(IvmRuntimeError::BindingSourceDescriptorMismatch(shape))
            if shape == "shared_params"
    ));
}

#[test]
fn graph_prepared_subscription_can_hide_internal_routing_fields() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.value_type())]);
    let binding = GraphBuilder::binding_source("hidden_title_eq_param", binding_descriptor);
    let graph = GraphBuilder::join(
        binding,
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("left.wanted", "__routing_wanted"),
    ]);
    let shape = database
        .prepare_one_sink(
            graph,
            "hidden_title_eq_param",
            binding_descriptor,
            ["__routing_wanted"],
        )
        .unwrap();
    let public_output = RecordDescriptor::new([
        ("id", ColumnType::U64.value_type()),
        ("title", ColumnType::String.value_type()),
    ]);
    let subscription = database
        .bind_shape_one_sink_with_output(
            shape.id(),
            &[Value::String("Blue Train".to_owned())],
            public_output,
        )
        .unwrap();

    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Out of Scope".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![11_u64.into(), "Blue Train".into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(11), Value::String("Blue Seven".to_owned())],
    );
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train".into()], -1),
            (vec![7_u64.into(), "Blue Train".into()], 1),
        ]
    );
}

#[test]
fn prepared_subscription_uses_route_terminal_with_clean_public_projection() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.value_type())]);
    let output_graph = GraphBuilder::table("albums")
        .project_fields([ProjectField::named("id"), ProjectField::named("title")]);
    let routing_graph = GraphBuilder::join(
        GraphBuilder::binding_source("explicit_route_title_param", binding_descriptor),
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("left.wanted", "__routing_wanted"),
    ]);
    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "explicit_route_title_param",
            binding_descriptor,
            ["__routing_wanted"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
        .unwrap();

    let initial = subscription.recv().unwrap();
    assert_eq!(
        initial.descriptor,
        RecordDescriptor::new([
            ("id", ColumnType::U64.value_type()),
            ("title", ColumnType::String.value_type()),
        ])
    );
    assert!(initial.is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Out of Scope".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![11_u64.into(), "Blue Train".into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(11), Value::String("Blue Seven".to_owned())],
    );
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![11_u64.into(), "Blue Train".into()], -1),
            (vec![7_u64.into(), "Blue Train".into()], 1),
        ]
    );
}

#[test]
fn prepared_subscription_routes_nullable_uuid_and_string_binding_keys() {
    let storage = MemoryStorage::new(&["docs"]);
    let mut database = Database::new(nullable_routed_docs_schema(), storage).unwrap();
    let owner = uuid(0x100);
    let other_owner = uuid(0x200);

    let mut batch = database.open_batch();
    batch.insert(
        "docs",
        vec![
            Value::U64(1),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::Nullable(Some(Box::new(Value::String("open".to_owned())))),
            Value::String("wanted".to_owned()),
        ],
    );
    batch.insert(
        "docs",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::Uuid(other_owner)))),
            Value::Nullable(Some(Box::new(Value::String("open".to_owned())))),
            Value::String("other owner".to_owned()),
        ],
    );
    batch.insert(
        "docs",
        vec![
            Value::U64(3),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::Nullable(Some(Box::new(Value::String("done".to_owned())))),
            Value::String("other tag".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([
        (
            "owner",
            ValueType::Nullable(Box::new(ColumnType::Uuid.value_type())),
        ),
        (
            "tag",
            ValueType::Nullable(Box::new(ColumnType::String.value_type())),
        ),
    ]);
    let output_graph = GraphBuilder::table("docs")
        .project_fields([ProjectField::named("id"), ProjectField::named("title")]);
    let routed_docs = GraphBuilder::table("docs")
        .unwrap_nullable("owner")
        .unwrap_nullable("tag")
        .project_fields([
            ProjectField::named("id"),
            ProjectField::named("title"),
            ProjectField::nullable("owner", "owner"),
            ProjectField::nullable("tag", "tag"),
        ]);
    let routing_graph = GraphBuilder::join(
        GraphBuilder::binding_source("nullable_doc_route", binding_descriptor),
        routed_docs,
        ["owner", "tag"],
        ["owner", "tag"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("right.owner", "__routing_owner"),
        ProjectField::renamed("right.tag", "__routing_tag"),
    ]);

    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "nullable_doc_route",
            binding_descriptor,
            ["__routing_owner", "__routing_tag"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(
            shape.id(),
            &[
                Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
                Value::Nullable(Some(Box::new(Value::String("open".to_owned())))),
            ],
        )
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::String("wanted".to_owned())], 1)]
    );
}

#[test]
fn prepared_subscription_routes_null_nullable_binding_keys() {
    let storage = MemoryStorage::new(&["docs"]);
    let mut database = Database::new(nullable_routed_docs_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "docs",
        vec![
            Value::U64(1),
            Value::Nullable(None),
            Value::Nullable(None),
            Value::String("null route".to_owned()),
        ],
    );
    batch.insert(
        "docs",
        vec![
            Value::U64(2),
            Value::Nullable(None),
            Value::Nullable(Some(Box::new(Value::String("open".to_owned())))),
            Value::String("partial null".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([
        (
            "owner",
            ValueType::Nullable(Box::new(ColumnType::Uuid.value_type())),
        ),
        (
            "tag",
            ValueType::Nullable(Box::new(ColumnType::String.value_type())),
        ),
    ]);
    let output_graph = GraphBuilder::table("docs")
        .project_fields([ProjectField::named("id"), ProjectField::named("title")]);
    let binding = GraphBuilder::binding_source("nullable_doc_null_route", binding_descriptor)
        .project_fields([
            ProjectField::named("owner"),
            ProjectField::named("tag"),
            ProjectField::literal("__join", Value::U8(0)),
        ]);
    let null_routing_graph = GraphBuilder::table("docs")
        .filter(PredicateExpr::is_null("owner"))
        .filter(PredicateExpr::is_null("tag"))
        .project_fields([
            ProjectField::named("id"),
            ProjectField::named("title"),
            ProjectField::literal("__join", Value::U8(0)),
            ProjectField::null_typed(
                "__routing_owner",
                ValueType::Nullable(Box::new(ValueType::Uuid)),
            ),
            ProjectField::null_typed(
                "__routing_tag",
                ValueType::Nullable(Box::new(ValueType::String)),
            ),
        ]);
    let null_routing_graph = GraphBuilder::join(
        binding,
        null_routing_graph,
        ["owner", "tag", "__join"],
        ["__routing_owner", "__routing_tag", "__join"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("right.__routing_owner", "__routing_owner"),
        ProjectField::renamed("right.__routing_tag", "__routing_tag"),
    ]);

    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            null_routing_graph,
            "nullable_doc_null_route",
            binding_descriptor,
            ["__routing_owner", "__routing_tag"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::Nullable(None), Value::Nullable(None)])
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![Value::U64(1), Value::String("null route".to_owned())],
            1
        )]
    );
}

#[test]
fn prepared_subscription_rejects_routing_graph_missing_clean_output_fields() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.value_type())]);
    let output_graph = GraphBuilder::table("albums")
        .project_fields([ProjectField::named("id"), ProjectField::named("title")]);
    let routing_graph = GraphBuilder::join(
        GraphBuilder::binding_source("missing_route_title_param", binding_descriptor),
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("left.wanted", "__routing_wanted"),
    ]);

    assert!(matches!(
        database.prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "missing_route_title_param",
            binding_descriptor,
            ["__routing_wanted"],
        ),
        Err(Error::IvmRuntime(IvmRuntimeError::GraphFieldNotFound(field))) if field == "title"
    ));
}

#[test]
fn prepared_subscription_with_separate_routing_hydrates_existing_rows_on_first_bind() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Out of Scope".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.value_type())]);
    let output_graph = GraphBuilder::join(
        GraphBuilder::binding_source("existing_route_title_param", binding_descriptor),
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
    ]);
    let routing_graph = GraphBuilder::join(
        GraphBuilder::binding_source("existing_route_title_param", binding_descriptor),
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("left.wanted", "__routing_wanted"),
    ]);
    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "existing_route_title_param",
            binding_descriptor,
            ["__routing_wanted"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[test]
fn prepared_recursive_subscription_with_separate_routing_hydrates_existing_rows_on_first_bind() {
    let storage = MemoryStorage::new(&["edges"]);
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 4, 5);
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.value_type())]);
    let output_graph = prepared_reachability_graph(GraphBuilder::table("edges"), 16);

    let reach = RecordDescriptor::new([
        ("seed", ColumnType::U64.value_type()),
        ("dst", ColumnType::U64.value_type()),
        ("__routing_seed", ColumnType::U64.value_type()),
    ]);
    let seed = GraphBuilder::binding_source("prepared-routed-reach", binding_descriptor)
        .project_fields([
            ProjectField::renamed("seed", "seed"),
            ProjectField::renamed("seed", "dst"),
            ProjectField::renamed("seed", "__routing_seed"),
        ]);
    let frontier = GraphBuilder::frontier_source("frontier", reach);
    let step = GraphBuilder::join(
        frontier,
        GraphBuilder::table("edges").project(["src", "dst"]),
        ["dst"],
        ["src"],
    )
    .project_fields([
        ProjectField::renamed("left.seed", "seed"),
        ProjectField::renamed("right.dst", "dst"),
        ProjectField::renamed("left.__routing_seed", "__routing_seed"),
    ]);
    let routing_graph = GraphBuilder::recursive(seed, step, "frontier", 16);

    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "prepared-routed-reach",
            binding_descriptor,
            ["__routing_seed"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();

    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);
    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(1)], 1),
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
        ]
    );
}

#[test]
fn prepared_recursive_subscription_joins_new_closure_to_preexisting_downstream_rows() {
    let storage = MemoryStorage::new(&["edges", "docs"]);
    let mut database = Database::new(edges_docs_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.value_type())]);
    let reach = prepared_reachability_graph(GraphBuilder::table("edges"), 16);
    let graph = GraphBuilder::join(GraphBuilder::table("docs"), reach, ["team"], ["dst"])
        .project_fields([
            ProjectField::renamed("left.id", "id"),
            ProjectField::renamed("left.team", "team"),
            ProjectField::renamed("right.seed", "seed"),
        ]);
    let shape = database
        .prepare_one_sink(graph, "prepared-reach", binding_descriptor, ["seed"])
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(11), Value::U64(3), Value::U64(1)], 1)]
    );
}

#[test]
fn routed_prepared_recursive_subscription_joins_new_closure_to_preexisting_downstream_rows() {
    let storage = MemoryStorage::new(&["edges", "docs"]);
    let mut database = Database::new(edges_docs_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.value_type())]);
    let reach = RecordDescriptor::new([
        ("seed", ColumnType::U64.value_type()),
        ("dst", ColumnType::U64.value_type()),
        ("__routing_seed", ColumnType::U64.value_type()),
    ]);
    let seed = GraphBuilder::binding_source("prepared-routed-reach-docs", binding_descriptor)
        .project_fields([
            ProjectField::renamed("seed", "seed"),
            ProjectField::renamed("seed", "dst"),
            ProjectField::renamed("seed", "__routing_seed"),
        ]);
    let frontier = GraphBuilder::frontier_source("frontier", reach);
    let step = GraphBuilder::join(
        frontier,
        GraphBuilder::table("edges").project(["src", "dst"]),
        ["dst"],
        ["src"],
    )
    .project_fields([
        ProjectField::renamed("left.seed", "seed"),
        ProjectField::renamed("right.dst", "dst"),
        ProjectField::renamed("left.__routing_seed", "__routing_seed"),
    ]);
    let reach = GraphBuilder::recursive(seed, step, "frontier", 16);
    let graph = GraphBuilder::join(GraphBuilder::table("docs"), reach, ["team"], ["dst"])
        .project_fields([
            ProjectField::renamed("left.id", "id"),
            ProjectField::renamed("left.team", "team"),
            ProjectField::renamed("right.seed", "seed"),
            ProjectField::renamed("right.__routing_seed", "__routing_seed"),
        ]);
    let shape = database
        .prepare(
            [RoutedMultisinkTerminal::new(
                "docs",
                graph,
                ["__routing_seed"],
                ["id", "team", "seed"],
            )],
            "prepared-routed-reach-docs",
            binding_descriptor,
        )
        .unwrap();
    let subscription = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();

    assert_eq!(
        subscription
            .recv()
            .unwrap()
            .get("docs")
            .unwrap()
            .to_values()
            .unwrap(),
        [(vec![Value::U64(11), Value::U64(3), Value::U64(1)], 1)]
    );
}

#[test]
fn routed_recursive_sibling_terminals_each_replay_positive_table_deltas() {
    fn routed_reach_graph(binding_shape: &str, route_field: &str) -> GraphBuilder {
        let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.value_type())]);
        let reach = RecordDescriptor::new([
            ("seed", ColumnType::U64.value_type()),
            ("dst", ColumnType::U64.value_type()),
            (route_field, ColumnType::U64.value_type()),
        ]);
        let seed =
            GraphBuilder::binding_source(binding_shape, binding_descriptor).project_fields([
                ProjectField::renamed("seed", "seed"),
                ProjectField::renamed("seed", "dst"),
                ProjectField::renamed("seed", route_field),
            ]);
        let frontier = GraphBuilder::frontier_source("frontier", reach);
        let step = GraphBuilder::join(
            frontier,
            GraphBuilder::table("edges").project(["src", "dst"]),
            ["dst"],
            ["src"],
        )
        .project_fields([
            ProjectField::renamed("left.seed", "seed"),
            ProjectField::renamed("right.dst", "dst"),
            ProjectField::renamed(format!("left.{route_field}"), route_field),
        ]);
        GraphBuilder::recursive(seed, step, "frontier", 16)
    }

    fn routed_docs_graph(binding_shape: &str, route_field: &str) -> GraphBuilder {
        let reach = routed_reach_graph(binding_shape, route_field);
        GraphBuilder::join(GraphBuilder::table("docs"), reach, ["team"], ["dst"]).project_fields([
            ProjectField::renamed("left.id", "id"),
            ProjectField::renamed("left.team", "team"),
            ProjectField::renamed("right.seed", "seed"),
            ProjectField::renamed(format!("right.{route_field}"), route_field),
        ])
    }

    let storage = MemoryStorage::new(&["edges", "docs"]);
    let mut database = Database::new(edges_docs_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.value_type())]);
    let shape = database
        .prepare(
            [
                RoutedMultisinkTerminal::new(
                    "route_seed",
                    routed_docs_graph("prepared-sibling-reach", "__routing_seed"),
                    ["__routing_seed"],
                    ["id", "team", "seed"],
                ),
                RoutedMultisinkTerminal::new(
                    "route_claim",
                    routed_docs_graph("prepared-sibling-reach", "__jazz_claim_sub"),
                    ["__jazz_claim_sub"],
                    ["id", "team", "seed"],
                ),
            ],
            "prepared-sibling-reach",
            binding_descriptor,
        )
        .unwrap();
    let subscription = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();

    let deltas = subscription.recv().unwrap();
    let expected = [(vec![Value::U64(11), Value::U64(3), Value::U64(1)], 1)];
    assert_eq!(
        deltas.get("route_seed").unwrap().to_values().unwrap(),
        expected
    );
    assert_eq!(
        deltas.get("route_claim").unwrap().to_values().unwrap(),
        expected
    );
}

#[test]
fn prepared_recursive_subscription_joins_two_simultaneous_closure_deltas() {
    let storage = MemoryStorage::new(&["edges", "docs"]);
    let mut database = Database::new(edges_docs_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(3)]);
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.value_type())]);
    let reach_descriptor = RecordDescriptor::new([
        ("seed", ColumnType::U64.value_type()),
        ("dst", ColumnType::U64.value_type()),
    ]);
    let reachable = |frontier_name: &str| {
        let seed = GraphBuilder::binding_source("prepared-double-reach", binding_descriptor)
            .project_fields([
                ProjectField::renamed("seed", "seed"),
                ProjectField::renamed("seed", "dst"),
            ]);
        let frontier = GraphBuilder::frontier_source(frontier_name, reach_descriptor);
        let step = GraphBuilder::join(
            frontier,
            GraphBuilder::table("edges").project(["src", "dst"]),
            ["dst"],
            ["src"],
        )
        .project_fields([
            ProjectField::renamed("left.seed", "seed"),
            ProjectField::renamed("right.dst", "dst"),
        ]);
        GraphBuilder::recursive(seed, step, frontier_name, 16)
    };
    let left_reach = reachable("frontier_a");
    let right_reach = reachable("frontier_b").project_fields([
        ProjectField::renamed("seed", "right_seed"),
        ProjectField::renamed("dst", "right_dst"),
    ]);
    let graph = GraphBuilder::join(GraphBuilder::table("docs"), left_reach, ["team"], ["dst"])
        .project_fields([
            ProjectField::renamed("left.id", "id"),
            ProjectField::renamed("left.team", "team"),
            ProjectField::renamed("right.seed", "seed"),
        ]);
    let graph = GraphBuilder::join(graph, right_reach, ["team"], ["right_dst"]).project_fields([
        ProjectField::renamed("left.id", "id"),
        ProjectField::renamed("left.team", "team"),
        ProjectField::renamed("left.seed", "seed"),
    ]);
    let shape = database
        .prepare_one_sink(graph, "prepared-double-reach", binding_descriptor, ["seed"])
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(11), Value::U64(3), Value::U64(1)], 1)]
    );
}

#[test]
fn prepared_recursive_grant_shape_joins_resource_and_access_added_in_one_tick() {
    fn run(split_ticks: bool) -> Vec<(Vec<Value>, i64)> {
        let storage = MemoryStorage::new(&["group_edges", "access_edges", "resources"]);
        let mut database = Database::new(grant_shape_schema(), storage).unwrap();
        let shape = prepare_grant_shape(&mut database);
        let subscription = database
            .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
            .unwrap();
        assert!(subscription.recv().unwrap().is_empty());

        if split_ticks {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            database.commit_batch(batch).unwrap();
            assert!(subscription.try_recv().is_err());

            let mut batch = database.open_batch();
            insert_access_edge(&mut batch, 20, 10, 1);
            database.commit_batch(batch).unwrap();
        } else {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            insert_access_edge(&mut batch, 20, 10, 1);
            database.commit_batch(batch).unwrap();
        }

        expect_recv_vals(&subscription)
    }

    assert_eq!(run(false), run(true));
}

#[test]
fn prepared_recursive_grant_shape_joins_membership_step_and_resource_in_one_tick() {
    fn run(split_ticks: bool) -> Vec<(Vec<Value>, i64)> {
        let storage = MemoryStorage::new(&["group_edges", "access_edges", "resources"]);
        let mut database = Database::new(grant_shape_schema(), storage).unwrap();
        let shape = prepare_grant_shape(&mut database);
        let subscription = database
            .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
            .unwrap();
        assert!(subscription.recv().unwrap().is_empty());

        if split_ticks {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            insert_access_edge(&mut batch, 20, 10, 2);
            database.commit_batch(batch).unwrap();
            assert!(subscription.try_recv().is_err());

            let mut batch = database.open_batch();
            insert_group_edge(&mut batch, 30, 1, 2);
            database.commit_batch(batch).unwrap();
        } else {
            let mut batch = database.open_batch();
            insert_resource(&mut batch, 10, 777);
            insert_access_edge(&mut batch, 20, 10, 2);
            insert_group_edge(&mut batch, 30, 1, 2);
            database.commit_batch(batch).unwrap();
        }

        expect_recv_vals(&subscription)
    }

    assert_eq!(run(false), run(true));
}

#[test]
fn prepared_subscription_with_routing_can_route_output_that_already_depends_on_binding() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("wanted", ColumnType::String.value_type())]);
    let output_graph = GraphBuilder::join(
        GraphBuilder::binding_source("double_route_title_param", binding_descriptor),
        GraphBuilder::table("albums"),
        ["wanted"],
        ["title"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
    ]);
    let routing_graph = GraphBuilder::join(
        output_graph.clone().project_fields([
            ProjectField::named("id"),
            ProjectField::named("title"),
            ProjectField::literal("__route_join", Value::U8(0)),
        ]),
        GraphBuilder::binding_source("double_route_title_param", binding_descriptor)
            .project_fields([
                ProjectField::named("wanted"),
                ProjectField::literal("__route_join", Value::U8(0)),
            ]),
        ["__route_join"],
        ["__route_join"],
    )
    .project_fields([
        ProjectField::renamed("left.id", "id"),
        ProjectField::renamed("left.title", "title"),
        ProjectField::renamed("right.wanted", "__routing_wanted"),
    ]);
    let shape = database
        .prepare_one_sink_with_routing(
            output_graph,
            routing_graph,
            "double_route_title_param",
            binding_descriptor,
            ["__routing_wanted"],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Blue Train".to_owned())])
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[test]
fn prepared_subscription_reports_incremental_contains_field_filter_deltas() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("needle", ColumnType::String.value_type())]);
    let routing_field = "__routing";
    let binding =
        GraphBuilder::binding_source("needle_param", binding_descriptor).project_fields([
            ProjectField::named("needle"),
            ProjectField::literal(routing_field, Value::U8(0)),
        ]);
    let albums = GraphBuilder::table("albums").project_fields([
        ProjectField::named("id"),
        ProjectField::named("title"),
        ProjectField::literal(routing_field, Value::U8(0)),
    ]);
    let graph = GraphBuilder::join(binding, albums, [routing_field], [routing_field])
        .project_fields([
            ProjectField::renamed("right.id", "id"),
            ProjectField::renamed("right.title", "title"),
            ProjectField::renamed("left.needle", "needle"),
        ])
        .filter(PredicateExpr::ContainsField {
            field: "title".to_owned(),
            needle_field: "needle".to_owned(),
        });
    let shape = database
        .prepare_one_sink(graph, "needle_param", binding_descriptor, ["needle"])
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::String("Train".to_owned())])
        .unwrap();

    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Out of Scope".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![
                11_u64.into(),
                "Blue Train".into(),
                Value::String("Train".to_owned()),
            ],
            1,
        )]
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(11), Value::String("Blue Seven".to_owned())],
    );
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Night Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (
                vec![
                    11_u64.into(),
                    "Blue Train".into(),
                    Value::String("Train".to_owned()),
                ],
                -1,
            ),
            (
                vec![
                    7_u64.into(),
                    "Night Train".into(),
                    Value::String("Train".to_owned()),
                ],
                1,
            ),
        ]
    );
}

#[test]
fn query_returns_filtered_current_rows() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Too Early".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let result = database
        .query(select_query(
            Select::new([SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    col("id"),
                    BinaryOp::Gt,
                    Expr::Literal(Value::U64(10)),
                )),
        ))
        .unwrap();
    assert_eq!(
        result.to_values().unwrap(),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[test]
fn enum_predicates_resolve_variant_names_at_plan_time() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tasks", "indices"]).unwrap();
    let mut database = Database::new(enum_tasks_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "tasks",
        vec![
            Value::U64(1),
            Value::String("todo".to_owned()),
            Value::Nullable(None),
            Value::String("one".to_owned()),
        ],
    );
    batch.insert(
        "tasks",
        vec![
            Value::U64(2),
            Value::String("done".to_owned()),
            Value::Nullable(Some(Box::new(Value::String("doing".to_owned())))),
            Value::String("two".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let result = database
        .query(select_query(
            Select::new([SelectItem::expr(col("title"))])
                .from([TableRef::named("tasks")])
                .where_(Expr::binary(
                    col("status"),
                    BinaryOp::Gt,
                    Expr::Literal(Value::String("todo".to_owned())),
                )),
        ))
        .unwrap();
    assert_eq!(result.to_values().unwrap(), [(vec!["two".into()], 1)]);
}

#[test]
fn enum_index_keys_follow_declaration_order() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tasks", "indices"]).unwrap();
    let mut database = Database::new(enum_tasks_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    for (id, status) in [(1, "done"), (2, "todo"), (3, "doing")] {
        batch.insert(
            "tasks",
            vec![
                Value::U64(id),
                Value::String(status.to_owned()),
                Value::Nullable(None),
                Value::String(format!("task-{id}")),
            ],
        );
    }
    database.commit_batch(batch).unwrap();

    assert_eq!(
        record_values(
            database
                .index_scan("tasks", "tasks_by_status", &[])
                .unwrap()
        )
        .into_iter()
        .map(|values| values[1].clone())
        .collect::<Vec<_>>(),
        vec![Value::Enum(0), Value::Enum(1), Value::Enum(2)]
    );
    assert_eq!(
        record_values(
            database
                .index_get(
                    "tasks",
                    "tasks_by_status",
                    &[Value::String("doing".to_owned())]
                )
                .unwrap()
        )
        .into_iter()
        .map(|values| values[3].clone())
        .collect::<Vec<_>>(),
        vec![Value::String("task-3".to_owned())]
    );
}

#[test]
fn nullable_comparisons_unwrap_present_values_and_skip_nulls() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["markers"]).unwrap();
    let mut database = Database::new(nullable_markers_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("markers", vec![Value::U64(1), Value::Nullable(None)]);
    batch.insert(
        "markers",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::String("deleted".to_owned())))),
        ],
    );
    database.commit_batch(batch).unwrap();

    let result = database
        .query(select_query(
            Select::new([SelectItem::expr(col("id"))])
                .from([TableRef::named("markers")])
                .where_(Expr::binary(
                    col("marker"),
                    BinaryOp::Eq,
                    Expr::Literal(Value::String("deleted".to_owned())),
                )),
        ))
        .unwrap();
    assert_eq!(result.to_values().unwrap(), [(vec![Value::U64(2)], 1)]);
}

#[test]
fn query_lowers_is_null_and_is_not_null_predicates() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["markers"]).unwrap();
    let mut database = Database::new(nullable_markers_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("markers", vec![Value::U64(1), Value::Nullable(None)]);
    batch.insert(
        "markers",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::String("present".to_owned())))),
        ],
    );
    database.commit_batch(batch).unwrap();

    let is_null = database
        .query(select_query(
            Select::new([SelectItem::expr(col("id"))])
                .from([TableRef::named("markers")])
                .where_(Expr::Unary {
                    op: UnaryOp::IsNull,
                    expr: Box::new(col("marker")),
                }),
        ))
        .unwrap();
    let is_not_null = database
        .query(select_query(
            Select::new([SelectItem::expr(col("id"))])
                .from([TableRef::named("markers")])
                .where_(Expr::Unary {
                    op: UnaryOp::IsNotNull,
                    expr: Box::new(col("marker")),
                }),
        ))
        .unwrap();

    assert_eq!(is_null.to_values().unwrap(), [(vec![Value::U64(1)], 1)]);
    assert_eq!(is_not_null.to_values().unwrap(), [(vec![Value::U64(2)], 1)]);
}

#[test]
fn is_null_matches_nested_nullable_none() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["markers"]).unwrap();
    let mut database = Database::new(nested_nullable_markers_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "markers",
        vec![
            Value::U64(1),
            Value::Nullable(Some(Box::new(Value::Nullable(None)))),
        ],
    );
    batch.insert(
        "markers",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::Nullable(Some(Box::new(
                Value::String("present".to_owned()),
            )))))),
        ],
    );
    database.commit_batch(batch).unwrap();

    let is_null = database
        .query(select_query(
            Select::new([SelectItem::expr(col("id"))])
                .from([TableRef::named("markers")])
                .where_(Expr::Unary {
                    op: UnaryOp::IsNull,
                    expr: Box::new(col("marker")),
                }),
        ))
        .unwrap();
    let is_not_null = database
        .query(select_query(
            Select::new([SelectItem::expr(col("id"))])
                .from([TableRef::named("markers")])
                .where_(Expr::Unary {
                    op: UnaryOp::IsNotNull,
                    expr: Box::new(col("marker")),
                }),
        ))
        .unwrap();

    assert_eq!(is_null.to_values().unwrap(), [(vec![Value::U64(1)], 1)]);
    assert_eq!(is_not_null.to_values().unwrap(), [(vec![Value::U64(2)], 1)]);
}

#[test]
fn unwrap_nullable_graph_drops_none_and_unwraps_present_values() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tracks", "indices"]).unwrap();
    let mut database = Database::new(indexed_tracks_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    batch.insert("tracks", track_values(2, 7, None, "Hidden"));
    batch.insert("tracks", track_values(3, 7, Some(2), "Outro"));
    database.commit_batch(batch).unwrap();

    let result = database
        .query_graph(
            GraphBuilder::table("tracks")
                .unwrap_nullable("disc")
                .project(["id", "disc"]),
        )
        .unwrap();
    assert_eq!(
        result.to_values().unwrap(),
        [
            (vec![Value::U64(1), Value::U64(1)], 1),
            (vec![Value::U64(3), Value::U64(2)], 1),
        ]
    );
}

#[test]
#[ignore = "receipt-only timing for batch-general schema index maintenance"]
fn indexed_batch_commit_timing_receipt_20k_and_single_row() {
    const ROWS: u64 = 20_000;
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tracks", "indices"]).unwrap();
    let mut database = Database::new(indexed_tracks_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    for id in 0..ROWS {
        batch.insert(
            "tracks",
            track_values(id, id % 30, Some(id % 5), &format!("bulk-track-{id:05}")),
        );
    }

    let bulk_start = Instant::now();
    database.commit_batch(batch).unwrap();
    let bulk_elapsed = bulk_start.elapsed();

    let album_rows = database
        .index_get(
            "tracks",
            "tracks_by_album_disc",
            &[
                Value::U64(7),
                Value::Nullable(Some(Box::new(Value::U64(2)))),
            ],
        )
        .unwrap();
    assert_eq!(album_rows.len(), 667);
    assert_eq!(
        database
            .index_get(
                "tracks",
                "tracks_by_title_unique",
                &[Value::String("bulk-track-12345".to_owned())],
            )
            .unwrap()
            .len(),
        1
    );

    let mut single = database.open_batch();
    single.insert(
        "tracks",
        track_values(ROWS + 1, 7, Some(2), "single-after-bulk"),
    );
    let single_start = Instant::now();
    database.commit_batch(single).unwrap();
    let single_elapsed = single_start.elapsed();

    println!(
        "indexed_batch_commit_timing_receipt rows={ROWS} bulk_ms={:.3} single_after_bulk_ms={:.3} matching_album_rows_after={}",
        bulk_elapsed.as_secs_f64() * 1000.0,
        single_elapsed.as_secs_f64() * 1000.0,
        database
            .index_get(
                "tracks",
                "tracks_by_album_disc",
                &[
                    Value::U64(7),
                    Value::Nullable(Some(Box::new(Value::U64(2)))),
                ],
            )
            .unwrap()
            .len()
    );
}

#[test]
fn query_graphs_returns_named_one_shot_snapshots() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(2), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let snapshots = database
        .query_graphs([
            ("ids", GraphBuilder::table("albums").project(["id"])),
            ("titles", GraphBuilder::table("albums").project(["title"])),
        ])
        .unwrap();

    assert_eq!(
        snapshots.get("ids").unwrap().to_values().unwrap(),
        [(vec![Value::U64(1)], 1), (vec![Value::U64(2)], 1)]
    );
    assert_eq!(
        snapshots.get("titles").unwrap().to_values().unwrap(),
        [
            (vec![Value::String("Blue Train".to_owned())], 1),
            (vec![Value::String("Giant Steps".to_owned())], 1)
        ]
    );
}

#[test]
fn unwrap_nullable_retractions_flow_symmetrically() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tracks", "indices"]).unwrap();
    let mut database = Database::new(indexed_tracks_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(
            GraphBuilder::table("tracks")
                .unwrap_nullable("disc")
                .project(["id", "disc"]),
        )
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("tracks", PrimaryKeyValue::U64(1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(1)], -1)]
    );
}

#[test]
fn project_nullable_wraps_uuid_and_string_fields() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["docs", "indices"]).unwrap();
    let mut database = Database::new(uuid_docs_schema(), storage).unwrap();
    let id = uuid(1);

    let mut batch = database.open_batch();
    batch.insert(
        "docs",
        vec![
            Value::Uuid(id),
            Value::Nullable(None),
            Value::String("draft".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let result = database
        .query_graph(GraphBuilder::table("docs").project_fields([
            ProjectField::nullable("id", "maybe_id"),
            ProjectField::nullable("title", "maybe_title"),
        ]))
        .unwrap();

    assert_eq!(
        result.to_values().unwrap(),
        [(
            vec![
                Value::Nullable(Some(Box::new(Value::Uuid(id)))),
                Value::Nullable(Some(Box::new(Value::String("draft".to_owned())))),
            ],
            1,
        )]
    );
}

#[test]
fn project_nullable_can_union_with_typed_null_projection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["docs", "indices"]).unwrap();
    let mut database = Database::new(uuid_docs_schema(), storage).unwrap();
    let id = uuid(2);

    let mut batch = database.open_batch();
    batch.insert(
        "docs",
        vec![
            Value::Uuid(id),
            Value::Nullable(None),
            Value::String("published".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let mut values = database
        .query_graph(GraphBuilder::union([
            GraphBuilder::table("docs").project_fields([
                ProjectField::nullable("id", "maybe_id"),
                ProjectField::nullable("title", "maybe_title"),
            ]),
            GraphBuilder::table("docs").project_fields([
                ProjectField::null_typed(
                    "maybe_id",
                    ValueType::Nullable(Box::new(ValueType::Uuid)),
                ),
                ProjectField::null_typed(
                    "maybe_title",
                    ValueType::Nullable(Box::new(ValueType::String)),
                ),
            ]),
        ]))
        .unwrap()
        .to_values()
        .unwrap();
    values.sort_by_key(|(values, _)| {
        if matches!(values[0], Value::Nullable(None)) {
            0
        } else {
            1
        }
    });

    assert_eq!(
        values,
        [
            (vec![Value::Nullable(None), Value::Nullable(None),], 1,),
            (
                vec![
                    Value::Nullable(Some(Box::new(Value::Uuid(id)))),
                    Value::Nullable(Some(Box::new(Value::String("published".to_owned())))),
                ],
                1,
            ),
        ]
    );
}

#[test]
fn arg_max_by_hydrates_and_tracks_winner_changes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "old"));
    batch.insert("history", history_values(1, 20, 1, "winner"));
    batch.insert("history", history_values(2, 5, 1, "other"));
    database.commit_batch(batch).unwrap();

    let subscription = database.subscribe_one_sink(history_arg_max()).unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[0] {
        Value::U64(row) => row,
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 20, 1, "winner"), 1),
            (history_values(2, 5, 1, "other"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 30, 1, "new"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "winner"), -1),
            (history_values(1, 30, 1, "new"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 30, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 30, 1, "new"), -1),
            (history_values(1, 20, 1, "winner"), 1),
        ]
    );
}

#[test]
fn arg_max_by_suppresses_non_winner_and_net_zero_deltas() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database.subscribe_one_sink(history_arg_max()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "winner"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "winner"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "loser"));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(2, 1, 1, "temporary"));
    batch.delete("history", history_key(2, 1, 1));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());
}

#[test]
fn arg_max_by_handles_multi_delta_same_group_and_tie_by_pk_order() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database.subscribe_one_sink(history_arg_max()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "a"));
    batch.insert("history", history_values(1, 10, 2, "b"));
    batch.insert("history", history_values(1, 9, 9, "c"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 2, "b"), 1)]
    );
}

#[test]
fn arg_min_by_hydrates_initial_snapshot_winner() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "later"));
    batch.insert("history", history_values(1, 10, 1, "winner"));
    batch.insert("history", history_values(2, 5, 1, "other"));
    database.commit_batch(batch).unwrap();

    let subscription = database.subscribe_one_sink(history_arg_min()).unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[0] {
        Value::U64(row) => row,
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 10, 1, "winner"), 1),
            (history_values(2, 5, 1, "other"), 1),
        ]
    );
}

#[test]
fn arg_min_by_tracks_lower_insert_and_current_winner_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database.subscribe_one_sink(history_arg_min()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "first"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "first"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "lower"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "first"), -1),
            (history_values(1, 10, 1, "lower"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 10, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 1, "lower"), -1),
            (history_values(1, 20, 1, "first"), 1),
        ]
    );
}

#[test]
fn arg_min_by_handles_same_tick_replacement_and_tie_by_pk_order() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database.subscribe_one_sink(history_arg_min()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 2, "old"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 2, "old"), 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 10, 2));
    batch.insert("history", history_values(1, 10, 1, "replacement"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 2, "old"), -1),
            (history_values(1, 10, 1, "replacement"), 1),
        ]
    );
}

#[test]
fn top_by_hydrates_limit_two() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 30, 1, "third"));
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(2))
        .unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[1] {
        Value::U64(stamp) => stamp,
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 10, 1, "first"), 1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );
}

#[test]
fn top_by_finite_zero_stays_empty() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(0))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());
}

#[test]
fn top_by_boundary_insert_and_delete_updates_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(2))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    batch.insert("history", history_values(1, 30, 1, "third"));
    database.commit_batch(batch).unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 15, 1, "middle"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "second"), -1),
            (history_values(1, 15, 1, "middle"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 15, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 15, 1, "middle"), -1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );
}

#[test]
fn top_by_suppresses_outside_window_changes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(2))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 30, 1, "outside"));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 30, 1));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());
}

#[test]
fn top_by_descending_order_keeps_largest_values() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    batch.insert("history", history_values(1, 30, 1, "third"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_desc(2))
        .unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[1] {
        Value::U64(stamp) => std::cmp::Reverse(stamp),
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 30, 1, "third"), 1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );
}

#[test]
fn top_by_offset_keeps_requested_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    batch.insert("history", history_values(1, 30, 1, "third"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc_offset(1, 1))
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "second"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 5, 1, "zeroth"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "second"), -1),
            (history_values(1, 10, 1, "first"), 1),
        ]
    );
}

#[test]
fn top_by_orders_nullable_sort_keys_null_first() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["scores"]).unwrap();
    let mut database = Database::new(nullable_scores_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "scores",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::U64(10)))),
            Value::String("ten".to_owned()),
        ],
    );
    batch.insert(
        "scores",
        vec![
            Value::U64(1),
            Value::Nullable(None),
            Value::String("null".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::top_by(
            GraphBuilder::table("scores"),
            std::iter::empty::<&str>(),
            [TopByOrder::asc("score")],
            ["id"],
            0,
            TopByLimit::Finite(1),
        ))
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(1),
                Value::Nullable(None),
                Value::String("null".to_owned()),
            ],
            1,
        )]
    );
}

#[test]
fn top_by_uses_stable_tie_field() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(1))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 2, "later tie"));
    batch.insert("history", history_values(1, 10, 1, "stable tie"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 1, "stable tie"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 0, "earlier tie"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 1, "stable tie"), -1),
            (history_values(1, 10, 0, "earlier tie"), 1),
        ]
    );
}

fn union_history_top_by(offset: u64, limit: u64) -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::union([
            GraphBuilder::table("history"),
            GraphBuilder::table("history_shadow"),
        ]),
        ["row"],
        [TopByOrder::asc("stamp")],
        ["node"],
        offset,
        TopByLimit::Finite(limit),
    )
}

#[test]
fn top_by_counts_duplicate_multiplicity_toward_window_occupancy() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "history_shadow"]).unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history_shadow", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(0, 2))
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 1, "first"), 2)]
    );
}

#[test]
fn top_by_offset_splits_duplicate_copies_across_boundary() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "history_shadow"]).unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history_shadow", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(1, 2))
        .unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[1] {
        Value::U64(stamp) => stamp,
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 10, 1, "first"), 1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );
}

#[test]
fn top_by_emits_weighted_diff_when_duplicate_copy_enters_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "history_shadow"]).unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(0, 2))
        .unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[1] {
        Value::U64(stamp) => stamp,
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 10, 1, "first"), 1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.insert("history_shadow", history_values(1, 10, 1, "first"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "second"), -1),
            (history_values(1, 10, 1, "first"), 1),
        ]
    );
}

#[test]
fn top_by_replaces_window_tie_with_distinct_record_on_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "history_shadow"]).unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "alpha"));
    batch.insert("history_shadow", history_values(1, 10, 1, "beta"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(0, 1))
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 1, "alpha"), 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 10, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 1, "alpha"), -1),
            (history_values(1, 10, 1, "beta"), 1),
        ]
    );
}

fn metric_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "metrics",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("bucket", ColumnType::U64),
            ColumnSchema::new("score", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn metric_values(id: u64, bucket: u64, score: u64) -> Vec<Value> {
    vec![Value::U64(id), Value::U64(bucket), Value::U64(score)]
}

fn metric_aggregate_graph(input: GraphBuilder) -> GraphBuilder {
    GraphBuilder::aggregate(
        input,
        ["bucket"],
        [
            AggregateExpr {
                function: AggregateFunction::Count,
                expression: None,
                distinct: false,
                output_name: Some("count".to_owned()),
            },
            AggregateExpr {
                function: AggregateFunction::Sum,
                expression: Some(PlanExpr::Field("score".to_owned())),
                distinct: false,
                output_name: Some("sum_score".to_owned()),
            },
            AggregateExpr {
                function: AggregateFunction::Avg,
                expression: Some(PlanExpr::Field("score".to_owned())),
                distinct: false,
                output_name: Some("avg_score".to_owned()),
            },
            AggregateExpr {
                function: AggregateFunction::Min,
                expression: Some(PlanExpr::Field("score".to_owned())),
                distinct: false,
                output_name: Some("min_score".to_owned()),
            },
            AggregateExpr {
                function: AggregateFunction::Max,
                expression: Some(PlanExpr::Field("score".to_owned())),
                distinct: false,
                output_name: Some("max_score".to_owned()),
            },
        ],
    )
}

fn metric_aggregate_table_graph() -> GraphBuilder {
    metric_aggregate_graph(GraphBuilder::table("metrics"))
}

fn sorted_values(mut values: Vec<(Vec<Value>, i64)>) -> Vec<(Vec<Value>, i64)> {
    values.sort_by(|left, right| format!("{:?}", left.0).cmp(&format!("{:?}", right.0)));
    values
}

fn materialized_values(
    materialized: &std::collections::BTreeMap<String, (Vec<Value>, i64)>,
) -> Vec<(Vec<Value>, i64)> {
    sorted_values(
        materialized
            .values()
            .filter_map(|(values, weight)| (*weight != 0).then_some((values.clone(), *weight)))
            .collect(),
    )
}

fn apply_materialized(
    materialized: &mut std::collections::BTreeMap<String, (Vec<Value>, i64)>,
    deltas: RecordDeltas,
) {
    for (values, weight) in deltas.to_values().unwrap() {
        let key = format!("{values:?}");
        let entry = materialized.entry(key).or_insert((values, 0));
        entry.1 += weight;
    }
    materialized.retain(|_, (_, weight)| *weight != 0);
}

#[test]
fn aggregate_hydrates_and_updates_group_summaries() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["metrics"]).unwrap();
    let mut database = Database::new(metric_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(metric_aggregate_table_graph())
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("metrics", metric_values(1, 10, 5));
    batch.insert("metrics", metric_values(2, 10, 7));
    batch.insert("metrics", metric_values(3, 20, 11));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        sorted_values(subscription.recv().unwrap().to_values().unwrap()),
        [
            (
                vec![
                    Value::U64(10),
                    Value::U64(2),
                    Value::U64(12),
                    Value::F64(6.0),
                    Value::U64(5),
                    Value::U64(7),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(20),
                    Value::U64(1),
                    Value::U64(11),
                    Value::F64(11.0),
                    Value::U64(11),
                    Value::U64(11),
                ],
                1,
            ),
        ]
    );

    let mut batch = database.open_batch();
    batch.update("metrics", metric_values(2, 10, 3));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        sorted_values(subscription.recv().unwrap().to_values().unwrap()),
        [
            (
                vec![
                    Value::U64(10),
                    Value::U64(2),
                    Value::U64(12),
                    Value::F64(6.0),
                    Value::U64(5),
                    Value::U64(7),
                ],
                -1,
            ),
            (
                vec![
                    Value::U64(10),
                    Value::U64(2),
                    Value::U64(8),
                    Value::F64(4.0),
                    Value::U64(3),
                    Value::U64(5),
                ],
                1,
            ),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("metrics", PrimaryKeyValue::U64(2));
    batch.delete("metrics", PrimaryKeyValue::U64(1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        sorted_values(subscription.recv().unwrap().to_values().unwrap()),
        [(
            vec![
                Value::U64(10),
                Value::U64(2),
                Value::U64(8),
                Value::F64(4.0),
                Value::U64(3),
                Value::U64(5),
            ],
            -1,
        )]
    );
}

#[test]
fn aggregate_counts_weighted_multiplicity_from_bag_union() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["metrics"]).unwrap();
    let mut database = Database::new(metric_schema(), storage).unwrap();
    let graph = metric_aggregate_graph(GraphBuilder::union([
        GraphBuilder::table("metrics"),
        GraphBuilder::table("metrics"),
    ]));
    let subscription = database.subscribe_one_sink(graph).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("metrics", metric_values(1, 10, 5));
    batch.insert("metrics", metric_values(2, 10, 7));
    database.commit_batch(batch).unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(10),
                Value::U64(4),
                Value::U64(24),
                Value::F64(6.0),
                Value::U64(5),
                Value::U64(7),
            ],
            1,
        )]
    );
}

#[test]
fn aggregate_incremental_matches_recompute_under_seeded_changes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["metrics"]).unwrap();
    let mut database = Database::new(metric_schema(), storage).unwrap();
    let graph = metric_aggregate_table_graph();
    let subscription = database.subscribe_one_sink(graph.clone()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut materialized = std::collections::BTreeMap::new();
    let mut live = std::collections::BTreeMap::<u64, (u64, u64)>::new();
    let mut seed = 0x5eed_u64;
    for step in 0..96 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = seed % 12;
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let op = seed % 5;
        let mut batch = database.open_batch();
        if op == 0 && live.contains_key(&id) {
            batch.delete("metrics", PrimaryKeyValue::U64(id));
            live.remove(&id);
        } else {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let bucket = 1 + (seed % 4);
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let score = 1 + (seed % 31);
            if live.contains_key(&id) {
                batch.update("metrics", metric_values(id, bucket, score));
            } else {
                batch.insert("metrics", metric_values(id, bucket, score));
            }
            live.insert(id, (bucket, score));
        }
        database.commit_batch(batch).unwrap();
        match subscription.try_recv() {
            Ok(emitted) => apply_materialized(&mut materialized, emitted),
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => panic!("aggregate subscription disconnected"),
        }
        let recomputed = sorted_values(
            database
                .query_graph(graph.clone())
                .unwrap()
                .to_values()
                .unwrap(),
        );
        assert_eq!(
            materialized_values(&materialized),
            recomputed,
            "step {step}, live {live:?}"
        );
    }
}

#[test]
fn aggregate_query_hydration_does_not_perturb_subscription_deltas() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["metrics"]).unwrap();
    let mut database = Database::new(metric_schema(), storage).unwrap();
    let graph = metric_aggregate_table_graph();
    let subscription = database.subscribe_one_sink(graph.clone()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    assert!(database.query_graph(graph.clone()).unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("metrics", metric_values(1, 10, 5));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(10),
                Value::U64(1),
                Value::U64(5),
                Value::F64(5.0),
                Value::U64(5),
                Value::U64(5),
            ],
            1,
        )]
    );

    let first = sorted_values(
        database
            .query_graph(graph.clone())
            .unwrap()
            .to_values()
            .unwrap(),
    );
    let second = sorted_values(database.query_graph(graph).unwrap().to_values().unwrap());
    assert_eq!(first, second);
}

#[test]
fn arg_max_by_feeds_join_and_anti_join() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let visible = database
        .subscribe_one_sink(GraphBuilder::anti_join(
            history_arg_max().project(["row", "stamp"]),
            GraphBuilder::table("blockers"),
            ["row"],
            ["row"],
        ))
        .unwrap();
    assert!(visible.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("rows", vec![Value::U64(1), Value::String("one".to_owned())]);
    batch.insert("history", history_values(1, 10, 1, "a"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        database
            .query_graph(
                GraphBuilder::join(
                    history_arg_max().project(["row", "stamp"]),
                    GraphBuilder::table("rows"),
                    ["row"],
                    ["row"],
                )
                .project_fields([
                    ProjectField::renamed("left.row", "row"),
                    ProjectField::renamed("left.stamp", "stamp"),
                    ProjectField::renamed("right.label", "label"),
                ]),
            )
            .unwrap()
            .to_values()
            .unwrap(),
        [(
            vec![
                Value::U64(1),
                Value::U64(10),
                Value::String("one".to_owned())
            ],
            1
        )]
    );
    assert_eq!(
        visible.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(10)], 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("blockers", vec![Value::U64(1)]);
    database.commit_batch(batch).unwrap();
    assert_eq!(
        visible.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(10)], -1)]
    );
}

#[test]
fn arg_max_by_routes_through_prepared_bindings() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let params = RecordDescriptor::new([("row", ColumnType::U64.value_type())]);
    let shape = database
        .prepare_one_sink(
            GraphBuilder::join(
                GraphBuilder::binding_source("row_param", params),
                history_arg_max().project(["row", "stamp"]),
                ["row"],
                ["row"],
            )
            .project_fields([
                ProjectField::renamed("left.row", "row"),
                ProjectField::renamed("right.stamp", "stamp"),
            ]),
            "row_param",
            params,
            ["row"],
        )
        .unwrap();
    let sub = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert!(sub.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "a"));
    batch.insert("history", history_values(2, 99, 1, "ignored"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        sub.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(10)], 1)]
    );
}

#[test]
fn arg_max_by_matches_naive_oracle_across_seeded_mutations() {
    #[derive(Clone)]
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0
        }
        fn range(&mut self, max: u64) -> u64 {
            self.next() % max
        }
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let mut rng = Lcg(0x0bad_cafe_1234_5678);
    let mut model = std::collections::BTreeMap::<(u64, u64, u64), String>::new();

    for _ in 0..160 {
        let mut batch = database.open_batch();
        for _ in 0..(1 + rng.range(4)) {
            let row = 1 + rng.range(8);
            let stamp = 1 + rng.range(32);
            let node = 1 + rng.range(4);
            let key = (row, stamp, node);
            if rng.range(5) == 0 {
                batch.delete("history", history_key(row, stamp, node));
                model.remove(&key);
            } else {
                let title = format!("v-{row}-{stamp}-{node}");
                if model.contains_key(&key) {
                    batch.update("history", history_values(row, stamp, node, &title));
                } else {
                    batch.insert("history", history_values(row, stamp, node, &title));
                }
                model.insert(key, title);
            }
        }
        database.commit_batch(batch).unwrap();

        let mut expected = std::collections::BTreeMap::<u64, (u64, u64, String)>::new();
        for (&(row, stamp, node), title) in &model {
            let entry = expected
                .entry(row)
                .or_insert_with(|| (stamp, node, title.clone()));
            if (stamp, node) > (entry.0, entry.1) {
                *entry = (stamp, node, title.clone());
            }
        }
        let mut expected = expected
            .into_iter()
            .map(|(row, (stamp, node, title))| (history_values(row, stamp, node, &title), 1))
            .collect::<Vec<_>>();
        expected.sort_by_key(|(values, _)| match &values[..] {
            [Value::U64(row), Value::U64(stamp), Value::U64(node), ..] => (*row, *stamp, *node),
            _ => unreachable!(),
        });

        let mut actual = database
            .query_graph(history_arg_max())
            .unwrap()
            .to_values()
            .unwrap();
        actual.sort_by_key(|(values, _)| match &values[..] {
            [Value::U64(row), Value::U64(stamp), Value::U64(node), ..] => (*row, *stamp, *node),
            _ => unreachable!(),
        });
        assert_eq!(actual, expected);
    }
}

#[test]
fn arg_max_by_tracks_union_of_filtered_sources() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "history_shadow"]).unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage).unwrap();
    let graph = GraphBuilder::arg_max_by(
        GraphBuilder::union([
            GraphBuilder::table("history").filter(PredicateExpr::gt("stamp", Value::U64(10))),
            GraphBuilder::table("history_shadow")
                .filter(PredicateExpr::gt("stamp", Value::U64(10))),
        ]),
        ["row"],
        ["stamp", "node"],
    );
    let subscription = database.subscribe_one_sink(graph.clone()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "left-winner"));
    batch.insert("history_shadow", history_values(1, 30, 1, "right-winner"));
    batch.insert("history_shadow", history_values(2, 40, 1, "other"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 30, 1, "right-winner"), 1),
            (history_values(2, 40, 1, "other"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("history_shadow", history_key(1, 30, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 30, 1, "right-winner"), -1),
            (history_values(1, 20, 1, "left-winner"), 1),
        ]
    );

    let mut actual = database.query_graph(graph).unwrap().to_values().unwrap();
    actual.sort_by_key(|(values, _)| match &values[..] {
        [Value::U64(row), Value::U64(stamp), Value::U64(node), ..] => (*row, *stamp, *node),
        _ => unreachable!(),
    });
    assert_eq!(
        actual,
        [
            (history_values(1, 20, 1, "left-winner"), 1),
            (history_values(2, 40, 1, "other"), 1),
        ]
    );
}

#[test]
fn arg_max_by_tracks_join_filter_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let joined_history = GraphBuilder::join(
        GraphBuilder::table("history"),
        GraphBuilder::table("rows").filter(PredicateExpr::eq(
            "label",
            Value::String("visible".to_owned()),
        )),
        ["row"],
        ["row"],
    )
    .project_fields([
        ProjectField::renamed("left.row", "row"),
        ProjectField::renamed("left.stamp", "stamp"),
        ProjectField::renamed("left.node", "node"),
        ProjectField::renamed("left.title", "title"),
    ]);
    let graph = GraphBuilder::arg_max_by(joined_history, ["row"], ["stamp", "node"]);
    let subscription = database.subscribe_one_sink(graph.clone()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "rows",
        vec![Value::U64(1), Value::String("visible".to_owned())],
    );
    batch.insert(
        "rows",
        vec![Value::U64(2), Value::String("hidden".to_owned())],
    );
    batch.insert("history", history_values(1, 10, 1, "old"));
    batch.insert("history", history_values(1, 20, 1, "winner"));
    batch.insert("history", history_values(2, 99, 1, "hidden"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "winner"), 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 20, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "winner"), -1),
            (history_values(1, 10, 1, "old"), 1),
        ]
    );

    let mut actual = database.query_graph(graph).unwrap().to_values().unwrap();
    actual.sort_by_key(|(values, _)| match &values[..] {
        [Value::U64(row), Value::U64(stamp), Value::U64(node), ..] => (*row, *stamp, *node),
        _ => unreachable!(),
    });
    assert_eq!(actual, [(history_values(1, 10, 1, "old"), 1)]);
}

#[test]
fn predicate_or_filter_matches_either_branch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let graph = GraphBuilder::table("albums").filter(
        PredicateExpr::Or(vec![
            PredicateExpr::eq("title", Value::String("Kind of Blue".to_owned())),
            PredicateExpr::gt("id", Value::U64(10)),
        ])
        .canonicalize(),
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Kind of Blue".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Speak No Evil".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let mut actual = database.query_graph(graph).unwrap().to_values().unwrap();
    actual.sort_by_key(|(values, _)| match &values[..] {
        [Value::U64(id), ..] => *id,
        _ => unreachable!(),
    });
    assert_eq!(
        actual,
        [
            (
                vec![Value::U64(1), Value::String("Kind of Blue".to_owned())],
                1
            ),
            (
                vec![Value::U64(11), Value::String("Speak No Evil".to_owned())],
                1
            ),
        ]
    );
}

#[test]
fn arg_max_by_rejects_unsupported_inputs_and_bad_primary_keys() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let err = database
        .subscribe_one_sink(GraphBuilder::arg_max_by(
            GraphBuilder::table("history"),
            ["row"],
            ["node", "stamp"],
        ))
        .unwrap_err();
    assert!(format!("{err}").contains("requires primary key"));

    database
        .subscribe_one_sink(GraphBuilder::recursive(
            history_arg_max().project(["row", "stamp"]),
            GraphBuilder::frontier_source(
                "frontier",
                RecordDescriptor::new([
                    ("row", ColumnType::U64.value_type()),
                    ("stamp", ColumnType::U64.value_type()),
                ]),
            ),
            "frontier",
            4,
        ))
        .unwrap();
}

#[test]
fn unwrap_nullable_can_feed_join_key() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tracks", "albums", "indices"]).unwrap();
    let mut tracks_schema = indexed_tracks_schema();
    let mut albums_schema = albums_schema();
    let mut database = Database::new(
        DatabaseSchema::new([
            tracks_schema.tables.remove(0),
            albums_schema.tables.remove(0),
        ]),
        storage,
    )
    .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("One".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(2), Value::String("Two".to_owned())],
    );
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    batch.insert("tracks", track_values(2, 7, None, "Hidden"));
    batch.insert("tracks", track_values(3, 7, Some(2), "Outro"));
    database.commit_batch(batch).unwrap();

    let mut values = database
        .query_graph(
            GraphBuilder::join(
                GraphBuilder::table("tracks").unwrap_nullable("disc"),
                GraphBuilder::table("albums"),
                ["disc"],
                ["id"],
            )
            .project_fields([
                ProjectField::renamed("left.id", "track_id"),
                ProjectField::renamed("right.title", "album_title"),
            ]),
        )
        .unwrap()
        .to_values()
        .unwrap();
    values.sort_by_key(|(values, _)| match &values[0] {
        Value::U64(value) => *value,
        other => panic!("expected track id, got {other:?}"),
    });
    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::String("One".to_owned())], 1),
            (vec![Value::U64(3), Value::String("Two".to_owned())], 1),
        ]
    );
}

#[test]
fn unwrap_nullable_can_feed_prepared_binding_join_key() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tracks", "indices"]).unwrap();
    let mut database = Database::new(indexed_tracks_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    batch.insert("tracks", track_values(2, 7, None, "Hidden"));
    batch.insert("tracks", track_values(3, 7, Some(2), "Outro"));
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("disc", ColumnType::U64.value_type())]);
    let shape = database
        .prepare_one_sink(
            GraphBuilder::join(
                GraphBuilder::binding_source("disc_param", binding_descriptor),
                GraphBuilder::table("tracks").unwrap_nullable("disc"),
                ["disc"],
                ["disc"],
            )
            .project_fields([
                ProjectField::renamed("right.id", "id"),
                ProjectField::renamed("right.disc", "disc"),
            ]),
            "disc_param",
            binding_descriptor,
            ["id"],
        )
        .unwrap();
    let disc_one = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert_eq!(
        expect_recv_vals(&disc_one),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );
}

#[test]
fn prepared_binding_join_hydrates_anti_join_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage =
        RocksDbStorage::open(temp_dir.path(), &["tracks", "blockers", "indices"]).unwrap();
    let schema = DatabaseSchema::new([
        indexed_tracks_schema().tables.remove(0),
        TableSchema::new("blockers", [ColumnSchema::new("id", ColumnType::U64)])
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ]);
    let mut database = Database::new(schema, storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    batch.insert("tracks", track_values(2, 7, Some(2), "Outro"));
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("disc", ColumnType::U64.value_type())]);
    let visible = GraphBuilder::anti_join(
        GraphBuilder::table("tracks").unwrap_nullable("disc"),
        GraphBuilder::table("blockers"),
        ["id"],
        ["id"],
    );
    let shape = database
        .prepare_one_sink(
            GraphBuilder::join(
                GraphBuilder::binding_source("disc_param", binding_descriptor),
                visible,
                ["disc"],
                ["disc"],
            )
            .project_fields([
                ProjectField::renamed("right.id", "id"),
                ProjectField::renamed("right.disc", "disc"),
            ]),
            "disc_param",
            binding_descriptor,
            ["id"],
        )
        .unwrap();
    let disc_one = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert_eq!(
        expect_recv_vals(&disc_one),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );
}

#[test]
fn prepared_binding_join_hydrates_filtered_unwrapped_anti_join_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["items", "blockers", "indices"]).unwrap();
    let schema = DatabaseSchema::new([
        TableSchema::new(
            "items",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("owner", ColumnType::Uuid.nullable()),
                ColumnSchema::new("state", ColumnType::String.nullable()),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new("blockers", [ColumnSchema::new("id", ColumnType::U64)])
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ]);
    let mut database = Database::new(schema, storage).unwrap();
    let owner = uuid::Uuid::from_bytes([1; 16]);

    let mut batch = database.open_batch();
    batch.insert(
        "items",
        vec![
            Value::U64(1),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::Nullable(Some(Box::new(Value::String("open".to_owned())))),
        ],
    );
    batch.insert(
        "items",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::Nullable(Some(Box::new(Value::String("done".to_owned())))),
        ],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor = RecordDescriptor::new([("owner", ColumnType::Uuid.value_type())]);
    let visible = GraphBuilder::anti_join(
        GraphBuilder::table("items")
            .unwrap_nullable("state")
            .filter(PredicateExpr::eq("state", Value::String("open".to_owned())))
            .unwrap_nullable("owner"),
        GraphBuilder::table("blockers"),
        ["id"],
        ["id"],
    );
    let shape = database
        .prepare_one_sink(
            GraphBuilder::join(
                GraphBuilder::binding_source("owner_param", binding_descriptor),
                visible,
                ["owner"],
                ["owner"],
            )
            .project_fields([
                ProjectField::renamed("left.owner", "owner"),
                ProjectField::renamed("right.id", "id"),
            ]),
            "owner_param",
            binding_descriptor,
            ["owner"],
        )
        .unwrap();
    let bound = database
        .bind_shape_one_sink(shape.id(), &[Value::Uuid(owner)])
        .unwrap();
    assert_eq!(
        expect_recv_vals(&bound),
        [(vec![Value::Uuid(owner), Value::U64(1)], 1)]
    );
}

#[test]
fn query_returns_empty_result_for_empty_answers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();

    let result = database
        .query(select_query(
            Select::new([SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    col("id"),
                    BinaryOp::Gt,
                    Expr::Literal(Value::U64(10)),
                )),
        ))
        .unwrap();

    assert!(result.is_empty());
}

#[test]
fn table_static_scan_specs_hydrate_like_full_scan_then_filter() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["docs", "indices"]).unwrap();
    let mut database = Database::new(scan_spec_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    insert_scan_doc(&mut batch, "a", 1, "/alpha", b"\0first");
    insert_scan_doc(&mut batch, "a", 2, "/beta", b"second");
    insert_scan_doc(&mut batch, "équipe", 1, "/unicode", b"\xffthird");
    insert_scan_doc(&mut batch, "z", 1, "/zeta", b"last");
    database.commit_batch(batch).unwrap();

    let prefix = database
        .query_graph(GraphBuilder::table_scan(
            "docs",
            StaticScanSpec::Prefix(vec![LiteralValue::String("a".to_owned())]),
        ))
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(
        prefix,
        [
            (
                vec![
                    Value::String("a".to_owned()),
                    Value::U64(1),
                    Value::String("/alpha".to_owned()),
                    Value::Bytes(b"\0first".to_vec()),
                ],
                1,
            ),
            (
                vec![
                    Value::String("a".to_owned()),
                    Value::U64(2),
                    Value::String("/beta".to_owned()),
                    Value::Bytes(b"second".to_vec()),
                ],
                1,
            ),
        ]
    );

    let point = database
        .query_graph(GraphBuilder::table_scan(
            "docs",
            StaticScanSpec::Point(vec![
                LiteralValue::String("équipe".to_owned()),
                LiteralValue::U64(1),
            ]),
        ))
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(point.len(), 1);
    assert_eq!(point[0].0[0], Value::String("équipe".to_owned()));

    let range = database
        .query_graph(GraphBuilder::table_scan(
            "docs",
            StaticScanSpec::Range {
                start: vec![LiteralValue::String("a".to_owned()), LiteralValue::U64(2)],
                end: vec![LiteralValue::String("z".to_owned())],
            },
        ))
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(
        range
            .into_iter()
            .map(|(row, _)| row[2].clone())
            .collect::<Vec<_>>(),
        [Value::String("/beta".to_owned())]
    );

    let empty = database
        .query_graph(GraphBuilder::table_scan(
            "docs",
            StaticScanSpec::Range {
                start: vec![LiteralValue::String("z".to_owned())],
                end: vec![LiteralValue::String("a".to_owned())],
            },
        ))
        .unwrap();
    assert!(empty.is_empty());
}

#[test]
fn index_static_scan_specs_filter_index_records() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["docs", "indices"]).unwrap();
    let mut database = Database::new(scan_spec_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    insert_scan_doc(&mut batch, "a", 1, "/alpha", b"first");
    insert_scan_doc(&mut batch, "b", 2, "/alpha", b"second");
    insert_scan_doc(&mut batch, "b", 3, "/beta", b"third");
    database.commit_batch(batch).unwrap();

    let prefix = database
        .query_graph(GraphBuilder::index_scan(
            "docs",
            "docs_by_path",
            StaticScanSpec::Prefix(vec![LiteralValue::String("/alpha".to_owned())]),
        ))
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(prefix.len(), 2);

    let point = database
        .query_graph(GraphBuilder::index_scan(
            "docs",
            "docs_by_path",
            StaticScanSpec::Point(vec![
                LiteralValue::String("/alpha".to_owned()),
                LiteralValue::String("b".to_owned()),
            ]),
        ))
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(point.len(), 1);

    let range = database
        .query_graph(GraphBuilder::index_scan(
            "docs",
            "docs_by_path",
            StaticScanSpec::Range {
                start: vec![LiteralValue::String("/alpha".to_owned())],
                end: vec![LiteralValue::String("/beta".to_owned())],
            },
        ))
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(range.len(), 2);
}

#[test]
fn static_scan_specs_participate_in_node_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["docs", "indices"]).unwrap();
    let mut database = Database::new(scan_spec_schema(), storage).unwrap();

    let same_a = GraphBuilder::table_scan(
        "docs",
        StaticScanSpec::Prefix(vec![LiteralValue::String("a".to_owned())]),
    );
    let same_b = same_a.clone();
    let different = GraphBuilder::table_scan(
        "docs",
        StaticScanSpec::Prefix(vec![LiteralValue::String("b".to_owned())]),
    );

    let first_subscription = database.subscribe_one_sink(same_a).unwrap();
    let after_first = database.ivm_runtime.graph().nodes().len();
    let second_subscription = database.subscribe_one_sink(same_b).unwrap();
    let after_same = database.ivm_runtime.graph().nodes().len();
    let different_subscription = database.subscribe_one_sink(different).unwrap();
    let after_different = database.ivm_runtime.graph().nodes().len();

    assert_eq!(after_first, after_same);
    assert!(after_different > after_same);
    drop((
        first_subscription,
        second_subscription,
        different_subscription,
    ));
}

#[test]
fn one_shot_static_scan_does_not_perturb_existing_subscription() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["docs", "indices"]).unwrap();
    let mut database = Database::new(scan_spec_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("docs").project(["tenant", "id"]))
        .unwrap();

    let mut batch = database.open_batch();
    insert_scan_doc(&mut batch, "a", 1, "/alpha", b"first");
    insert_scan_doc(&mut batch, "b", 2, "/beta", b"second");
    database.commit_batch(batch).unwrap();
    let initial = expect_recv_vals(&subscription);
    assert_eq!(initial.len(), 2);

    let queried = database
        .query_graph(GraphBuilder::table_scan(
            "docs",
            StaticScanSpec::Prefix(vec![LiteralValue::String("a".to_owned())]),
        ))
        .unwrap();
    assert_eq!(queried.deltas.len(), 1);

    let mut batch = database.open_batch();
    insert_scan_doc(&mut batch, "c", 3, "/gamma", b"third");
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::String("c".to_owned()), Value::U64(3)], 1)]
    );
}

#[test]
fn subscribe_supports_recursive_hydration_snapshot_message() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();

    let subscription = database.subscribe_one_sink(reachability_graph(16)).unwrap();
    database.flush().unwrap();
    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
            (vec![Value::U64(2), Value::U64(3)], 1),
        ]
    );

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(4)], 1),
            (vec![Value::U64(2), Value::U64(4)], 1),
            (vec![Value::U64(3), Value::U64(4)], 1),
        ]
    );
}

#[test]
fn same_key_writes_in_one_batch_emit_deltas_against_earlier_batch_writes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec![7_u64.into(), "Giant Steps".into()], 1)]
    );
    let stored = database
        .storage
        .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(
        database
            .ivm_runtime
            .schema()
            .table("albums")
            .unwrap()
            .record_schema()
            .get(&stored, "title")
            .unwrap(),
        Value::String("Giant Steps".to_owned())
    );
}

#[test]
fn inserts_over_existing_primary_keys_are_rejected() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(indexed_albums_schema(), storage).unwrap();
    database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    database
        .subscribe_one_sink(GraphBuilder::index("albums", "albums_by_title"))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    let err = database.commit_batch(batch).unwrap_err();

    assert!(matches!(err, Error::DuplicatePrimaryKey { table, .. } if table == "albums"));
    let stored = database
        .storage
        .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(
        database
            .ivm_runtime
            .schema()
            .table("albums")
            .unwrap()
            .record_schema()
            .get(&stored, "title")
            .unwrap(),
        Value::String("Blue Train".to_owned())
    );
}

#[test]
fn inserts_over_primary_keys_created_earlier_in_the_same_batch_are_rejected() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    let err = database.commit_batch(batch).unwrap_err();

    assert!(matches!(err, Error::DuplicatePrimaryKey { table, .. } if table == "albums"));
    assert!(
        database
            .storage
            .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
            .unwrap()
            .is_none()
    );
}

#[test]
fn same_batch_same_key_operations_emit_only_the_consolidated_final_delta() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(
            vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
            1
        )]
    );
}

#[test]
fn query_subscriptions_receive_filtered_projected_messages() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_query(select_query(
            Select::new([SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    col("id"),
                    BinaryOp::Gt,
                    Expr::Literal(Value::U64(10)),
                )),
        ))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Too Early".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[test]
fn query_projection_aliases_drive_output_schema() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_query(select_query(
            Select::new([SelectItem::aliased(col("title"), "album_title")])
                .from([TableRef::named("albums")]),
        ))
        .unwrap();
    let output = database
        .ivm_runtime
        .subscription_output(subscription_id.id())
        .unwrap();
    assert_eq!(output.fields()[0].name.as_deref(), Some("album_title"));

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[test]
fn query_subscriptions_can_read_from_simple_ctes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let cte = Cte::new(
        "recent",
        select_query(
            Select::new([SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    col("id"),
                    BinaryOp::GtEq,
                    Expr::Literal(Value::U64(10)),
                )),
        ),
    );
    let subscription_id = database
        .subscribe_query(Query::With(Box::new(WithQuery::new(
            [cte],
            select_query(
                Select::new([SelectItem::aliased(col("title"), "recent_title")])
                    .from([TableRef::named("recent")]),
            ),
        ))))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Too Early".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(10), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[test]
fn query_subscriptions_support_literal_on_left_predicates() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_query(select_query(
            Select::new([SelectItem::expr(col("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    Expr::Literal(Value::U64(10)),
                    BinaryOp::Lt,
                    col("id"),
                )),
        ))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(10), Value::String("Boundary".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[test]
fn query_subscriptions_support_multi_key_inner_joins() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(tenant_albums_artists_schema(), storage).unwrap();
    let join = TableRef::Join {
        left: Box::new(TableRef::named("albums").aliased("a")),
        right: Box::new(TableRef::named("artists").aliased("r")),
        kind: JoinKind::Inner,
        constraint: JoinConstraint::On(Expr::binary(
            Expr::binary(qcol("a", "tenant_id"), BinaryOp::Eq, qcol("r", "tenant_id")),
            BinaryOp::And,
            Expr::binary(qcol("a", "artist_id"), BinaryOp::Eq, qcol("r", "id")),
        )),
    };
    let subscription_id = database
        .subscribe_query(select_query(
            Select::new([
                SelectItem::aliased(qcol("a", "title"), "album_title"),
                SelectItem::aliased(qcol("r", "name"), "artist_name"),
            ])
            .from([join]),
        ))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::String("Coltrane".to_owned()),
        ],
    );
    batch.insert(
        "artists",
        vec![
            Value::U64(2),
            Value::U64(8),
            Value::String("Wrong Tenant".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();
    assert!(subscription_id.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(42),
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec!["Blue Train".into(), "Coltrane".into()], 1)]
    );
}

#[test]
fn query_subscriptions_support_qualified_wildcards_after_join() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let join = TableRef::Join {
        left: Box::new(TableRef::named("albums").aliased("a")),
        right: Box::new(TableRef::named("artists").aliased("r")),
        kind: JoinKind::Inner,
        constraint: JoinConstraint::On(Expr::binary(
            qcol("a", "artist_id"),
            BinaryOp::Eq,
            qcol("r", "id"),
        )),
    };
    let subscription_id = database
        .subscribe_query(select_query(
            Select::new([SelectItem::QualifiedWildcard(vec!["a".to_owned()])]).from([join]),
        ))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[test]
fn recursive_graph_subscriptions_settle_transitive_closure_in_one_tick() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
    let mut values = expect_recv_vals(&subscription_id);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
            (vec![Value::U64(1), Value::U64(4)], 1),
            (vec![Value::U64(2), Value::U64(3)], 1),
            (vec![Value::U64(2), Value::U64(4)], 1),
            (vec![Value::U64(3), Value::U64(4)], 1),
        ]
    );
}

#[test]
fn recursive_graph_subscriptions_retract_derived_paths_after_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        1
    );
    let _initial_reach = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(2));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        1
    );
    let mut values = expect_recv_vals(&subscription_id);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], -1),
            (vec![Value::U64(2), Value::U64(3)], -1),
            (vec![Value::U64(2), Value::U64(4)], -1),
        ]
    );
}

#[test]
fn prepared_recursive_binding_retracts_transitive_paths_after_edge_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let shape = prepared_reachability_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(2));
    database.commit_batch(batch).unwrap();
    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], -1),
        ]
    );
}

#[test]
fn prepared_recursive_binding_skips_recompute_for_unrelated_table_delta() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "docs"]).unwrap();
    let mut database = Database::new(edges_docs_schema(), storage).unwrap();
    let shape = database
        .prepare_one_sink(
            prepared_reachability_graph(GraphBuilder::table("edges"), 16),
            "prepared-reach",
            RecordDescriptor::new([("seed", ColumnType::U64.value_type())]),
            ["seed".to_owned()],
        )
        .unwrap();
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();
    let mut initial = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut initial);
    assert_eq!(
        initial,
        [
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(11), Value::U64(99)]);
    database.commit_batch(batch).unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        0
    );
    assert!(subscription.try_recv().is_err());
}

#[test]
fn prepared_recursive_binding_recomputes_for_relevant_insert_and_retraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let shape = prepared_reachability_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    database.commit_batch(batch).unwrap();
    // Sanctioned by ARC 2 step-delta recursion instruction: the insert half
    // used to pin a recompute mechanism, not semantics. Positive step-table
    // inserts now run semi-naive incrementally; retractions below still
    // recompute.
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        0
    );
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(2)], 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        1
    );
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(2)], -1)]
    );
}

#[test]
fn prepared_recursive_positive_step_inserts_match_recompute_diff_without_recompute() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let shape = prepared_reachability_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let mut edges = Vec::<(u64, u64)>::new();
    let mut previous = prepared_reachability_oracle(1, &edges);
    let inserts = seeded_positive_edge_insertions();
    for (idx, (src, dst)) in inserts.into_iter().enumerate() {
        let mut batch = database.open_batch();
        insert_edge(&mut batch, idx as u64 + 1, src, dst);
        database.commit_batch(batch).unwrap();
        assert_eq!(
            database
                .last_commit_metrics()
                .unwrap()
                .tick
                .recursive_recomputes,
            0,
            "positive prepared recursive step insert should not recompute at index {idx}: {src}->{dst}"
        );

        edges.push((src, dst));
        let next = prepared_reachability_oracle(1, &edges);
        let mut expected = next
            .difference(&previous)
            .map(|dst| (vec![Value::U64(1), Value::U64(*dst)], 1))
            .collect::<Vec<_>>();
        sort_pairs_by_value(&mut expected);

        if expected.is_empty() {
            assert!(
                subscription.try_recv().is_err(),
                "already-known/re-derived edge {src}->{dst} should emit no recursive delta"
            );
        } else {
            let mut actual = expect_recv_vals(&subscription);
            sort_pairs_by_value(&mut actual);
            assert_eq!(
                actual, expected,
                "positive recursive step insert {src}->{dst} must match recompute diff"
            );
        }
        previous = next;
    }
}

#[test]
fn prepared_recursive_binding_retracts_paths_after_first_edge_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let shape = prepared_reachability_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.delete("edges", PrimaryKeyValue::U64(1));
    database.commit_batch(batch).unwrap();
    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(2)], -1),
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], -1),
        ]
    );
}

#[test]
fn prepared_recursive_binding_retraction_recomputes_instead_of_erroring() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let shape = prepared_reachability_shape(&mut database);
    let first = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    let _empty = first.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 9, 10);
    insert_edge(&mut batch, 4, 5, 6);
    database.commit_batch(batch).unwrap();

    let mut initial = expect_recv_vals(&first);
    sort_pairs_by_value(&mut initial);
    assert_eq!(
        initial,
        [
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
        ]
    );

    let second = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(9)])
        .unwrap();
    let mut next = expect_recv_vals(&second);
    sort_pairs_by_value(&mut next);
    assert_eq!(
        next,
        [
            (vec![Value::U64(9), Value::U64(9)], 1),
            (vec![Value::U64(9), Value::U64(10)], 1),
        ]
    );

    drop(first);
    let mut batch = database.open_batch();
    insert_edge(&mut batch, 5, 3, 4);
    database.commit_batch(batch).unwrap();

    database.flush().unwrap();
    assert_eq!(
        database.last_tick_metrics().unwrap().recursive_recomputes,
        1
    );

    let third = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(5)])
        .unwrap();
    let mut third_values = expect_recv_vals(&third);
    sort_pairs_by_value(&mut third_values);
    assert_eq!(
        third_values,
        [
            (vec![Value::U64(5), Value::U64(5)], 1),
            (vec![Value::U64(5), Value::U64(6)], 1),
        ]
    );
}

#[test]
fn prepared_recursive_binding_retracts_transitive_paths_from_antijoin_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "blockers"]).unwrap();
    let mut database = Database::new(edges_blockers_schema(), storage).unwrap();
    let shape = prepared_reachability_with_antijoin_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(1), Value::U64(2), Value::U64(3)],
    );
    database.commit_batch(batch).unwrap();
    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], -1),
        ]
    );
}

#[test]
fn prepared_recursive_binding_retracts_first_paths_from_antijoin_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "blockers"]).unwrap();
    let mut database = Database::new(edges_blockers_schema(), storage).unwrap();
    let shape = prepared_reachability_with_antijoin_shape(&mut database);
    let subscription = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(1)])
        .unwrap();
    let _empty = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();
    let _initial = expect_recv_vals(&subscription);

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(1), Value::U64(1), Value::U64(2)],
    );
    database.commit_batch(batch).unwrap();
    let mut values = expect_recv_vals(&subscription);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(2)], -1),
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], -1),
        ]
    );
}

#[test]
fn recursive_graph_subscriptions_collapse_duplicate_derivations() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 1, 3);
    insert_edge(&mut batch, 3, 2, 4);
    insert_edge(&mut batch, 4, 3, 4);
    database.commit_batch(batch).unwrap();
    let values = expect_recv_vals(&subscription_id);

    assert!(values.contains(&(vec![Value::U64(1), Value::U64(4)], 1)));
}

#[test]
fn recursive_graph_subscriptions_recompute_after_edge_update() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();
    let _initial_reach = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    update_edge(&mut batch, 2, 2, 4);
    database.commit_batch(batch).unwrap();
    let mut values = expect_recv_vals(&subscription_id);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(3)], -1),
            (vec![Value::U64(1), Value::U64(4)], 1),
            (vec![Value::U64(2), Value::U64(3)], -1),
            (vec![Value::U64(2), Value::U64(4)], 1),
        ]
    );
}

#[test]
fn recursive_graph_subscriptions_incrementally_extend_existing_reach_with_new_edge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    database.commit_batch(batch).unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        1
    );
    let _initial_reach = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .recursive_recomputes,
        0
    );
    let mut values = expect_recv_vals(&subscription_id);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(3)], 1),
            (vec![Value::U64(2), Value::U64(3)], 1),
        ]
    );
}

#[test]
fn recursive_graph_subscriptions_incrementally_extend_new_seed_with_existing_edge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription_id = database.subscribe_one_sink(reachability_graph(16)).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 2, 3);
    database.commit_batch(batch).unwrap();
    let _initial_reach = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 2, 1, 2);
    database.commit_batch(batch).unwrap();
    let mut values = expect_recv_vals(&subscription_id);
    sort_pairs_by_value(&mut values);

    assert_eq!(
        values,
        [
            (vec![Value::U64(1), Value::U64(2)], 1),
            (vec![Value::U64(1), Value::U64(3)], 1),
        ]
    );
}

#[test]
fn recursive_graph_subscriptions_converge_on_self_cycles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let subscription = database.subscribe_one_sink(reachability_graph(2)).unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 1);
    database.commit_batch(batch).unwrap();
    let values = subscription.recv().unwrap().to_values().unwrap();

    assert_eq!(values, [(vec![Value::U64(1), Value::U64(1)], 1)]);
}

#[test]
fn recursive_graphs_reject_seed_and_step_output_descriptor_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let frontier = GraphBuilder::frontier_source(
        "frontier",
        RecordDescriptor::new([
            ("src", ColumnType::U64.value_type()),
            ("dst", ColumnType::U64.value_type()),
        ]),
    );
    let step = frontier.project(["src"]);
    let graph = GraphBuilder::recursive(
        GraphBuilder::table("edges").project(["src", "dst"]),
        step,
        "frontier",
        16,
    );

    assert!(matches!(
        database.subscribe_one_sink(graph).unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::GraphOutputMismatch)
    ));
}

#[test]
fn recursive_graphs_reject_nested_recursion_for_v0() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let reach = RecordDescriptor::new([
        ("src", ColumnType::U64.value_type()),
        ("dst", ColumnType::U64.value_type()),
    ]);
    let graph = GraphBuilder::recursive(
        reachability_graph(16),
        GraphBuilder::frontier_source("outer-frontier", reach),
        "outer-frontier",
        4,
    );

    assert!(matches!(
        database.subscribe_one_sink(graph).unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::UnsupportedNestedRecursion)
    ));
}

#[test]
fn recursive_graphs_fail_when_frontier_exceeds_max_iters() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    insert_edge(&mut batch, 3, 3, 4);
    database.commit_batch(batch).unwrap();

    assert!(matches!(
        database.query_graph(reachability_graph(1)).unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::RecursiveIterationLimit { max_iters: 1, .. })
    ));
}

#[test]
fn duplicate_table_subscriptions_share_graph_nodes_and_gc_eagerly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();

    let first = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    let second = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    let first_output = database
        .ivm_runtime
        .subscription_output_node(first.id())
        .unwrap();
    let second_output = database
        .ivm_runtime
        .subscription_output_node(second.id())
        .unwrap();

    assert_eq!(first_output, second_output);
    assert_eq!(database.ivm_runtime.retained_node_ids().len(), 1);

    assert!(database.unsubscribe(first.id()));
    assert!(database.ivm_runtime.graph().node(first_output).is_some());

    assert!(database.unsubscribe(second.id()));
    assert!(database.ivm_runtime.graph().node(first_output).is_none());
    assert!(database.ivm_runtime.retained_node_ids().is_empty());
}

#[test]
fn union_subscriptions_receive_deltas_from_multiple_tables() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "archived_albums"]).unwrap();
    let mut database = Database::new(two_album_tables_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::union([
            GraphBuilder::table("albums"),
            GraphBuilder::table("archived_albums"),
        ]))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "archived_albums",
        vec![Value::U64(2), Value::String("Out to Lunch".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id)
            .into_iter()
            .map(|(values, _)| values)
            .collect::<Vec<_>>(),
        [
            vec![1_u64.into(), "Blue Train".into()],
            vec![2_u64.into(), "Out to Lunch".into()]
        ]
    );
}

#[test]
fn union_all_subscriptions_preserve_duplicate_derivations() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let album_titles = GraphBuilder::table("albums").project(["title"]);
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::union([album_titles.clone(), album_titles]))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [
            (vec!["Blue Train".into()], 1),
            (vec!["Blue Train".into()], 1)
        ]
    );
}

#[test]
fn filter_subscriptions_emit_only_matching_rows() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(
            GraphBuilder::table("albums").filter(PredicateExpr::gt("id", Value::U64(10))),
        )
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec![11_u64.into(), "Giant Steps".into()], 1)]
    );
}

#[test]
fn project_subscriptions_emit_projected_records() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::table("albums").project(["title"]))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[test]
fn duplicate_projected_subscriptions_share_graph_nodes_and_gc_eagerly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let graph = GraphBuilder::table("albums")
        .filter(PredicateExpr::eq(
            "title",
            Value::String("Blue Train".to_owned()),
        ))
        .project(["title"]);

    let first = database.subscribe_one_sink(graph.clone()).unwrap();
    let second = database.subscribe_one_sink(graph).unwrap();
    let first_output = database
        .ivm_runtime
        .subscription_output_node(first.id())
        .unwrap();
    let second_output = database
        .ivm_runtime
        .subscription_output_node(second.id())
        .unwrap();

    assert_eq!(first_output, second_output);
    assert!(
        database
            .ivm_runtime
            .retained_node_ids()
            .contains(&first_output)
    );

    assert!(database.unsubscribe(first.id()));
    assert!(database.ivm_runtime.graph().node(first_output).is_some());

    assert!(database.unsubscribe(second.id()));
    assert!(database.ivm_runtime.graph().node(first_output).is_none());
    assert!(database.ivm_runtime.retained_node_ids().is_empty());
}

#[test]
fn join_subscriptions_match_left_deltas_against_maintained_right_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    assert!(subscription_id.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(
            vec![
                7_u64.into(),
                11_u64.into(),
                "Blue Train".into(),
                11_u64.into(),
                "John Coltrane".into(),
            ],
            1
        )]
    );
}

#[test]
fn join_subscriptions_match_array_key_elements() {
    let storage = MemoryStorage::new(&["files", "file_parts"]);
    let mut database = Database::new(files_parts_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("files"),
            GraphBuilder::table("file_parts"),
            ["part_ids"],
            ["part_uuid"],
        ))
        .unwrap();

    let part_a = uuid(0xa);
    let part_b = uuid(0xb);
    let part_c = uuid(0xc);

    let mut batch = database.open_batch();
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Array(vec![Value::Uuid(part_a), Value::Uuid(part_b)]),
        ],
    );
    batch.insert(
        "file_parts",
        vec![
            Value::U64(10),
            Value::Uuid(part_b),
            Value::Bytes(b"b".to_vec()),
        ],
    );
    batch.insert(
        "file_parts",
        vec![
            Value::U64(11),
            Value::Uuid(part_c),
            Value::Bytes(b"c".to_vec()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(
            vec![
                Value::U64(1),
                Value::Array(vec![Value::Uuid(part_a), Value::Uuid(part_b)]),
                Value::U64(10),
                Value::Uuid(part_b),
                Value::Bytes(b"b".to_vec()),
            ],
            1
        )]
    );
}

#[test]
fn unnest_subscription_emits_one_row_per_array_element() {
    let storage = MemoryStorage::new(&["files", "file_parts"]);
    let mut database = Database::new(files_parts_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(
            GraphBuilder::table("files")
                .unnest("part_ids", "part_id")
                .project(["id", "part_id"]),
        )
        .unwrap();

    let part_a = uuid(0xa);
    let part_b = uuid(0xb);
    let part_c = uuid(0xc);

    let mut batch = database.open_batch();
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Array(vec![Value::Uuid(part_a), Value::Uuid(part_b)]),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![Value::U64(1), Value::Uuid(part_a)], 1),
            (vec![Value::U64(1), Value::Uuid(part_b)], 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("files", PrimaryKeyValue::U64(1));
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Array(vec![Value::Uuid(part_b), Value::Uuid(part_c)]),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![Value::U64(1), Value::Uuid(part_a)], -1),
            (vec![Value::U64(1), Value::Uuid(part_b)], -1),
            (vec![Value::U64(1), Value::Uuid(part_b)], 1),
            (vec![Value::U64(1), Value::Uuid(part_c)], 1),
        ]
    );
}

#[test]
fn join_subscriptions_match_persisted_array_key_elements() {
    let storage = MemoryStorage::new(&["files", "file_parts"]);
    let mut database = Database::new(files_parts_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("files"),
            GraphBuilder::table("file_parts"),
            ["part_ids"],
            ["part_uuid"],
        ))
        .unwrap();

    let part_a = uuid(0xa);
    let part_b = uuid(0xb);
    let part_c = uuid(0xc);

    let mut batch = database.open_batch();
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Array(vec![
                Value::Uuid(part_b),
                Value::Uuid(part_b),
                Value::Uuid(part_a),
            ]),
        ],
    );
    batch.insert("files", vec![Value::U64(2), Value::Array(vec![])]);
    database.commit_batch(batch).unwrap();
    assert!(subscription_id.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "file_parts",
        vec![
            Value::U64(10),
            Value::Uuid(part_b),
            Value::Bytes(b"b".to_vec()),
        ],
    );
    batch.insert(
        "file_parts",
        vec![
            Value::U64(11),
            Value::Uuid(part_a),
            Value::Bytes(b"a".to_vec()),
        ],
    );
    batch.insert(
        "file_parts",
        vec![
            Value::U64(12),
            Value::Uuid(part_c),
            Value::Bytes(b"c".to_vec()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [
            (
                vec![
                    Value::U64(1),
                    Value::Array(vec![
                        Value::Uuid(part_b),
                        Value::Uuid(part_b),
                        Value::Uuid(part_a),
                    ]),
                    Value::U64(10),
                    Value::Uuid(part_b),
                    Value::Bytes(b"b".to_vec()),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(1),
                    Value::Array(vec![
                        Value::Uuid(part_b),
                        Value::Uuid(part_b),
                        Value::Uuid(part_a),
                    ]),
                    Value::U64(11),
                    Value::Uuid(part_a),
                    Value::Bytes(b"a".to_vec()),
                ],
                1,
            ),
        ]
    );
}

#[test]
fn join_subscriptions_match_nullable_array_key_elements() {
    let storage = MemoryStorage::new(&["files", "file_parts"]);
    let mut database = Database::new(nullable_files_parts_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("files"),
            GraphBuilder::table("file_parts"),
            ["part_ids"],
            ["part_uuid"],
        ))
        .unwrap();

    let part_a = uuid(0xa);
    let part_b = uuid(0xb);

    let mut batch = database.open_batch();
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Nullable(Some(Box::new(Value::Array(vec![
                Value::Uuid(part_a),
                Value::Uuid(part_b),
            ])))),
        ],
    );
    batch.insert(
        "file_parts",
        vec![
            Value::U64(10),
            Value::Nullable(Some(Box::new(Value::Uuid(part_b)))),
            Value::Bytes(b"b".to_vec()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(
            vec![
                Value::U64(1),
                Value::Nullable(Some(Box::new(Value::Array(vec![
                    Value::Uuid(part_a),
                    Value::Uuid(part_b),
                ])))),
                Value::U64(10),
                Value::Nullable(Some(Box::new(Value::Uuid(part_b)))),
                Value::Bytes(b"b".to_vec()),
            ],
            1
        )]
    );
}

#[test]
fn index_subscriptions_expand_array_key_elements() {
    let storage = MemoryStorage::new(&["files", "indices"]);
    let mut database = Database::new(indexed_files_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::index("files", "files_by_part_ids"))
        .unwrap();

    let part_a = uuid(0xa);
    let part_b = uuid(0xb);
    let mut batch = database.open_batch();
    batch.insert(
        "files",
        vec![
            Value::U64(1),
            Value::Array(vec![Value::Uuid(part_b), Value::Uuid(part_a)]),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (
                vec![
                    encoded_uuid_index_key(part_a, 1).into(),
                    Vec::<u8>::new().into(),
                ],
                1,
            ),
            (
                vec![
                    encoded_uuid_index_key(part_b, 1).into(),
                    Vec::<u8>::new().into(),
                ],
                1,
            ),
        ]
    );
}

#[test]
fn query_graph_joins_related_tables_through_database_facade() {
    let storage = MemoryStorage::new(&["albums", "artists"]);
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(1), Value::String("John Coltrane".to_owned())],
    );
    batch.insert(
        "artists",
        vec![Value::U64(2), Value::String("Miles Davis".to_owned())],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(10),
            Value::U64(1),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(11),
            Value::U64(2),
            Value::String("Kind of Blue".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(12),
            Value::U64(1),
            Value::String("Giant Steps".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let rows = database
        .query_graph(
            GraphBuilder::join(
                GraphBuilder::table("albums"),
                GraphBuilder::table("artists"),
                ["artist_id"],
                ["id"],
            )
            .project_fields([
                ProjectField::renamed("right.name", "artist"),
                ProjectField::renamed("left.title", "album"),
            ]),
        )
        .unwrap();

    let mut values = rows.to_values().unwrap();
    values.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
    assert_eq!(
        values,
        [
            (
                vec![
                    Value::String("John Coltrane".to_owned()),
                    Value::String("Blue Train".to_owned()),
                ],
                1,
            ),
            (
                vec![
                    Value::String("John Coltrane".to_owned()),
                    Value::String("Giant Steps".to_owned()),
                ],
                1,
            ),
            (
                vec![
                    Value::String("Miles Davis".to_owned()),
                    Value::String("Kind of Blue".to_owned()),
                ],
                1,
            ),
        ]
    );
}

#[test]
fn join_subscriptions_match_right_deltas_against_maintained_left_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();
    assert!(subscription_id.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(
            vec![
                7_u64.into(),
                11_u64.into(),
                "Blue Train".into(),
                11_u64.into(),
                "John Coltrane".into(),
            ],
            1
        )]
    );
}

#[test]
fn join_subscriptions_emit_update_and_delete_deltas_from_maintained_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();
    let _initial_join = expect_recv_vals(&subscription_id);

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Giant Steps".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let deltas = expect_recv_vals(&subscription_id);
    assert_eq!(deltas.len(), 2);
    assert!(deltas.contains(&(
        vec![
            7_u64.into(),
            11_u64.into(),
            "Blue Train".into(),
            11_u64.into(),
            "John Coltrane".into(),
        ],
        -1
    )));
    assert!(deltas.contains(&(
        vec![
            7_u64.into(),
            11_u64.into(),
            "Giant Steps".into(),
            11_u64.into(),
            "John Coltrane".into(),
        ],
        1
    )));

    let mut batch = database.open_batch();
    batch.delete("artists", PrimaryKeyValue::U64(11));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(
            vec![
                7_u64.into(),
                11_u64.into(),
                "Giant Steps".into(),
                11_u64.into(),
                "John Coltrane".into(),
            ],
            -1
        )]
    );
}

#[test]
fn anti_join_subscriptions_emit_left_rows_without_right_matches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::anti_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[test]
fn semi_join_subscriptions_emit_left_rows_with_right_matches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::semi_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[test]
fn semi_join_retracts_and_restores_on_right_threshold_transitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::semi_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("artists", PrimaryKeyValue::U64(11));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], -1)]
    );
}

#[test]
fn semi_join_hydration_snapshot_filters_missing_right_matches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(8),
            Value::U64(12),
            Value::String("Unknown Session".to_owned()),
        ],
    );
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::semi_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[test]
fn anti_join_retracts_and_restores_on_right_threshold_transitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::anti_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(expect_recv_vals(&subscription)[0].1, 1);

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], -1)]
    );

    let mut batch = database.open_batch();
    batch.delete("artists", PrimaryKeyValue::U64(11));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[test]
fn anti_join_only_changes_when_right_count_crosses_zero() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "blocks"]).unwrap();
    let mut database = Database::new(albums_blockers_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::anti_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("blocks"),
            ["artist_id"],
            ["artist_id"],
        ))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(expect_recv_vals(&subscription)[0].1, 1);

    let mut batch = database.open_batch();
    batch.insert("blocks", vec![Value::U64(1), Value::U64(11)]);
    batch.insert("blocks", vec![Value::U64(2), Value::U64(11)]);
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], -1)]
    );

    let mut batch = database.open_batch();
    batch.delete("blocks", PrimaryKeyValue::U64(1));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.delete("blocks", PrimaryKeyValue::U64(2));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![7_u64.into(), 11_u64.into(), "Blue Train".into()], 1)]
    );
}

#[test]
fn anti_join_hydration_snapshot_filters_existing_right_matches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(8),
            Value::U64(12),
            Value::String("Unknown Session".to_owned()),
        ],
    );
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::anti_join(
            GraphBuilder::table("albums"),
            GraphBuilder::table("artists"),
            ["artist_id"],
            ["id"],
        ))
        .unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(
            vec![8_u64.into(), 12_u64.into(), "Unknown Session".into()],
            1
        )]
    );
}

#[test]
fn anti_join_filters_identical_descriptors_before_projection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "blockers"]).unwrap();
    let mut database = Database::new(edges_blockers_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(4), Value::U64(3)]);
    batch.insert(
        "blockers",
        vec![Value::U64(5), Value::U64(4), Value::U64(3)],
    );
    batch.insert("edges", vec![Value::U64(2), Value::U64(8), Value::U64(4)]);
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(8), Value::U64(4)], 1)]
    );
}

#[test]
fn anti_join_hydration_snapshot_filters_many_existing_identical_descriptor_blockers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "blockers"]).unwrap();
    let mut database = Database::new(edges_blockers_schema(), storage).unwrap();

    let edges = [
        (1, 8, 4),
        (3, 2, 5),
        (4, 4, 3),
        (9, 4, 7),
        (10, 6, 2),
        (11, 4, 8),
        (18, 6, 3),
        (19, 8, 1),
        (20, 7, 6),
    ];
    let blockers = [
        (5, 4, 3),
        (6, 6, 3),
        (7, 2, 3),
        (9, 3, 3),
        (13, 8, 1),
        (17, 1, 2),
        (21, 7, 1),
        (22, 2, 2),
    ];
    let mut batch = database.open_batch();
    for (id, src, dst) in edges {
        batch.insert(
            "edges",
            vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
        );
    }
    for (id, src, dst) in blockers {
        batch.insert(
            "blockers",
            vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
        );
    }
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [
            (vec![Value::U64(2), Value::U64(5)], 1),
            (vec![Value::U64(4), Value::U64(7)], 1),
            (vec![Value::U64(4), Value::U64(8)], 1),
            (vec![Value::U64(6), Value::U64(2)], 1),
            (vec![Value::U64(7), Value::U64(6)], 1),
            (vec![Value::U64(8), Value::U64(4)], 1),
        ]
    );
}

#[test]
fn anti_join_retracts_identical_descriptor_projection_when_blocker_arrives() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "blockers"]).unwrap();
    let mut database = Database::new(edges_blockers_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(4), Value::U64(3)]);
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(4), Value::U64(3)], 1)]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(5), Value::U64(4), Value::U64(3)],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(4), Value::U64(3)], -1)]
    );
}

#[test]
fn anti_join_remembers_blocker_inserted_before_matching_left_key_exists() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "blockers"]).unwrap();
    let mut database = Database::new(edges_blockers_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(8), Value::U64(4)]);
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(8), Value::U64(4)], 1)]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(5), Value::U64(4), Value::U64(3)],
    );
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.update("edges", vec![Value::U64(1), Value::U64(4), Value::U64(3)]);
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(8), Value::U64(4)], -1)]
    );
}

#[test]
fn anti_join_retracts_when_right_update_moves_onto_left_key() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "blockers"]).unwrap();
    let mut database = Database::new(edges_blockers_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(4), Value::U64(4), Value::U64(3)]);
    batch.insert(
        "blockers",
        vec![Value::U64(5), Value::U64(6), Value::U64(8)],
    );
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(4), Value::U64(3)], 1)]
    );

    let mut batch = database.open_batch();
    batch.update(
        "blockers",
        vec![Value::U64(5), Value::U64(4), Value::U64(3)],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(4), Value::U64(3)], -1)]
    );
}

#[test]
fn anti_join_resubscribe_hydrates_from_storage_after_unretained_changes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "blockers"]).unwrap();
    let mut database = Database::new(edges_blockers_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("edges", vec![Value::U64(1), Value::U64(4), Value::U64(3)]);
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::U64(4), Value::U64(3)], 1)]
    );
    assert!(database.unsubscribe(subscription.id()));

    let mut batch = database.open_batch();
    batch.insert(
        "blockers",
        vec![Value::U64(5), Value::U64(4), Value::U64(3)],
    );
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(unblocked_edges_graph())
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());
}

#[test]
fn parameterized_shape_hydrates_and_routes_by_param() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(2),
            Value::U64(8),
            Value::String("Kind of Blue".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let shape = database
        .prepare_one_sink(
            artist_album_shape_graph(),
            "artist_params",
            artist_binding_descriptor(),
            ["artist_id"],
        )
        .unwrap();
    let coltrane = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(7)])
        .unwrap();
    let miles = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(8)])
        .unwrap();

    assert_eq!(
        expect_try_recv_vals(&coltrane),
        vec![(
            vec![
                Value::U64(7),
                Value::U64(1),
                Value::String("Blue Train".to_owned())
            ],
            1
        )]
    );
    assert_eq!(
        expect_try_recv_vals(&miles),
        vec![(
            vec![
                Value::U64(8),
                Value::U64(2),
                Value::String("Kind of Blue".to_owned())
            ],
            1
        )]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(3),
            Value::U64(7),
            Value::String("Giant Steps".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_try_recv_vals(&coltrane),
        vec![(
            vec![
                Value::U64(7),
                Value::U64(3),
                Value::String("Giant Steps".to_owned())
            ],
            1
        )]
    );
    assert!(miles.try_recv().is_err());
}

#[test]
fn parameterized_shape_uses_set_semantics_with_duplicate_param_refcounts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let shape = database
        .prepare_one_sink(
            artist_album_shape_graph(),
            "artist_params",
            artist_binding_descriptor(),
            ["artist_id"],
        )
        .unwrap();
    let first = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(7)])
        .unwrap();
    let second = database
        .bind_shape_one_sink(shape.id(), &[Value::U64(7)])
        .unwrap();

    assert_eq!(expect_try_recv_vals(&first).len(), 1);
    assert_eq!(expect_try_recv_vals(&second).len(), 1);

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(2),
            Value::U64(7),
            Value::String("Giant Steps".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let first_delta = expect_try_recv_vals(&first);
    let second_delta = expect_try_recv_vals(&second);
    assert_eq!(first_delta, second_delta);
    assert_eq!(first_delta[0].1, 1);

    assert!(database.unsubscribe(first.id()));
    assert!(second.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(3),
            Value::U64(7),
            Value::String("A Love Supreme".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_try_recv_vals(&second),
        vec![(
            vec![
                Value::U64(7),
                Value::U64(3),
                Value::String("A Love Supreme".to_owned())
            ],
            1
        )]
    );
}

#[test]
fn prepared_subscription_lowers_parameter_predicates_to_shape_subscriptions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(2),
            Value::U64(8),
            Value::String("Kind of Blue".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let query = select_query(
        Select::new([
            SelectItem::expr(Expr::column("id")),
            SelectItem::expr(Expr::column("title")),
        ])
        .from([TableRef::named("albums")])
        .where_(Expr::binary(
            Expr::column("artist_id"),
            BinaryOp::Eq,
            Expr::parameter("artist"),
        )),
    );
    assert!(database.subscribe_query(query.clone()).is_err());

    let prepared = database.prepare_query(query).unwrap();
    assert_eq!(prepared.parameters()[0].name, "artist");
    assert_eq!(
        prepared
            .output()
            .fields()
            .iter()
            .filter_map(|field| field.name.as_deref())
            .collect::<Vec<_>>(),
        vec!["id", "title"]
    );
    let sub = database
        .bind(&prepared, &[("artist", Value::U64(7))])
        .unwrap();
    let other = database
        .bind(&prepared, &[("artist", Value::U64(8))])
        .unwrap();
    assert_eq!(
        database
            .ivm_runtime
            .subscription_output(sub.id())
            .unwrap()
            .fields()
            .iter()
            .filter_map(|field| field.name.as_deref())
            .collect::<Vec<_>>(),
        vec!["id", "title"]
    );

    assert_eq!(
        expect_try_recv_vals(&sub),
        vec![(
            vec![Value::U64(1), Value::String("Blue Train".to_owned())],
            1
        )]
    );
    assert_eq!(
        expect_try_recv_vals(&other),
        vec![(
            vec![Value::U64(2), Value::String("Kind of Blue".to_owned())],
            1
        )]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(3),
            Value::U64(7),
            Value::String("Giant Steps".to_owned()),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(4),
            Value::U64(8),
            Value::String("Milestones".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_try_recv_vals(&sub),
        vec![(
            vec![Value::U64(3), Value::String("Giant Steps".to_owned())],
            1
        )]
    );
    assert_eq!(
        expect_try_recv_vals(&other),
        vec![(
            vec![Value::U64(4), Value::String("Milestones".to_owned())],
            1
        )]
    );
}

#[test]
fn prepared_subscription_filters_not_equal_parameter_predicates() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(2), Value::String("Kind of Blue".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let binding_descriptor =
        RecordDescriptor::new([("title_param", ColumnType::String.value_type())]);
    let graph = GraphBuilder::join(
        GraphBuilder::binding_source("title_neq_params", binding_descriptor).project_fields([
            ProjectField::named("title_param"),
            ProjectField::literal("__route", Value::U8(0)),
        ]),
        GraphBuilder::table("albums").project_fields([
            ProjectField::named("id"),
            ProjectField::named("title"),
            ProjectField::literal("__route", Value::U8(0)),
        ]),
        ["__route"],
        ["__route"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("left.title_param", "title_param"),
    ])
    .filter(PredicateExpr::NeqField {
        field: "title".to_owned(),
        value_field: "title_param".to_owned(),
    });
    let prepared = database
        .prepare_one_sink(
            graph,
            "title_neq_params",
            binding_descriptor,
            ["title_param"],
        )
        .unwrap();
    let sub = database
        .bind_shape_one_sink(prepared.id(), &[Value::String("Blue Train".to_owned())])
        .unwrap();

    assert_eq!(
        expect_try_recv_vals(&sub),
        vec![(
            vec![
                Value::U64(2),
                Value::String("Kind of Blue".to_owned()),
                Value::String("Blue Train".to_owned()),
            ],
            1,
        )]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(3), Value::String("Giant Steps".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(4), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(
        expect_try_recv_vals(&sub),
        vec![(
            vec![
                Value::U64(3),
                Value::String("Giant Steps".to_owned()),
                Value::String("Blue Train".to_owned()),
            ],
            1,
        )]
    );
}

#[test]
fn prepare_query_requires_parameters_and_only_lowers_parameter_equalities() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();

    let no_parameters = select_query(
        Select::new([SelectItem::expr(Expr::column("id"))])
            .from([TableRef::named("albums")])
            .where_(Expr::binary(
                Expr::column("artist_id"),
                BinaryOp::Eq,
                Expr::Literal(Value::U64(7)),
            )),
    );
    assert!(matches!(
        database.prepare_query(no_parameters).unwrap_err(),
        Error::QueryPlanning(PlannerError::UnsupportedQuery(
            "prepare_query requires at least one query parameter"
        ))
    ));

    let non_equality_parameter = select_query(
        Select::new([SelectItem::expr(Expr::column("id"))])
            .from([TableRef::named("albums")])
            .where_(Expr::binary(
                Expr::column("artist_id"),
                BinaryOp::Gt,
                Expr::parameter("artist"),
            )),
    );
    assert!(matches!(
        database.prepare_query(non_equality_parameter).unwrap_err(),
        Error::QueryPlanning(PlannerError::UnsupportedExpression(
            "only equality parameter predicates are supported"
        ))
    ));

    let parameter_to_parameter = select_query(
        Select::new([SelectItem::expr(Expr::column("id"))])
            .from([TableRef::named("albums")])
            .where_(Expr::binary(
                Expr::parameter("artist"),
                BinaryOp::Eq,
                Expr::parameter("other"),
            )),
    );
    assert!(matches!(
        database.prepare_query(parameter_to_parameter).unwrap_err(),
        Error::QueryPlanning(PlannerError::UnsupportedExpression(
            "only column = parameter predicates are supported"
        ))
    ));
}

#[test]
fn select_literal_and_null_projections_remain_unsupported_by_query_planner() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();

    for expr in [Expr::Null, Expr::Literal(Value::String("x".to_owned()))] {
        let query =
            select_query(Select::new([SelectItem::expr(expr)]).from([TableRef::named("albums")]));

        assert!(matches!(
            database.subscribe_query(query).unwrap_err(),
            Error::QueryPlanning(PlannerError::UnsupportedExpression(
                "only column projection is currently lowerable"
            ))
        ));
    }
}

#[test]
fn prepared_subscription_validates_named_bindings() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let prepared = database
        .prepare_query(select_query(
            Select::new([SelectItem::expr(Expr::column("id"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    Expr::column("artist_id"),
                    BinaryOp::Eq,
                    Expr::parameter("artist"),
                )),
        ))
        .unwrap();

    assert!(
        database
            .bind(&prepared, &[("other", Value::U64(7))])
            .is_err()
    );
    assert!(
        database
            .bind(&prepared, &[("artist", Value::String("nope".to_owned()))])
            .is_err()
    );
}

#[test]
fn graph_level_prepare_rejects_output_key_fields_not_in_output_descriptor() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("artist_id", ColumnType::U64.value_type())]);
    let graph = GraphBuilder::join(
        GraphBuilder::binding_source("artist_params", binding_descriptor),
        GraphBuilder::table("albums"),
        ["artist_id"],
        ["artist_id"],
    )
    .project_fields([
        ProjectField::renamed("right.artist_id", "artist_id"),
        ProjectField::renamed("right.id", "id"),
    ]);

    assert!(matches!(
        database
            .prepare_one_sink(graph, "artist_params", binding_descriptor, ["missing"])
            .unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::ShapeKeyFieldNotFound(field)) if field == "missing"
    ));
}

#[test]
fn prepared_shapes_retain_output_graph_nodes_without_subscribers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let binding_descriptor = RecordDescriptor::new([("artist_id", ColumnType::U64.value_type())]);
    let graph = GraphBuilder::join(
        GraphBuilder::binding_source("artist_params", binding_descriptor),
        GraphBuilder::table("albums"),
        ["artist_id"],
        ["artist_id"],
    )
    .project_fields([
        ProjectField::renamed("right.artist_id", "artist_id"),
        ProjectField::renamed("right.id", "id"),
    ]);

    let _shape = database
        .prepare_one_sink(graph, "artist_params", binding_descriptor, ["artist_id"])
        .unwrap();
    let retained = database.ivm_runtime.retained_node_ids();
    let retained_output_nodes = retained
        .iter()
        .filter(|node| {
            database
                .ivm_runtime
                .graph()
                .node(**node)
                .is_some_and(|graph_node| graph_node.children.is_empty())
        })
        .collect::<Vec<_>>();

    assert_eq!(retained_output_nodes.len(), 1);
    assert!(
        database
            .ivm_runtime
            .graph()
            .node(*retained_output_nodes[0])
            .is_some()
    );
    assert_eq!(database.ivm_runtime.stats().active_prepared_shapes, 1);
}

#[test]
fn prepared_subscription_matches_literal_subscription_without_param_columns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let param_query = select_query(
        Select::new([
            SelectItem::expr(Expr::column("id")),
            SelectItem::expr(Expr::column("title")),
        ])
        .from([TableRef::named("albums")])
        .where_(Expr::binary(
            Expr::column("artist_id"),
            BinaryOp::Eq,
            Expr::parameter("artist"),
        )),
    );
    let literal_query = select_query(
        Select::new([
            SelectItem::expr(Expr::column("id")),
            SelectItem::expr(Expr::column("title")),
        ])
        .from([TableRef::named("albums")])
        .where_(Expr::binary(
            Expr::column("artist_id"),
            BinaryOp::Eq,
            Expr::Literal(Value::U64(7)),
        )),
    );
    let prepared = database.prepare_query(param_query).unwrap();
    let param_sub = database
        .bind(&prepared, &[("artist", Value::U64(7))])
        .unwrap();
    let literal_sub = database.subscribe_query(literal_query).unwrap();

    assert_eq!(
        expect_try_recv_vals(&param_sub),
        expect_try_recv_vals(&literal_sub)
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(10),
            Value::U64(7),
            Value::String("Interstellar Space".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_try_recv_vals(&param_sub),
        expect_try_recv_vals(&literal_sub)
    );
}

#[test]
fn prepared_subscriptions_match_literal_subscriptions_under_seeded_interleavings() {
    for seed in [0x7117_u64, 0x5151_u64, 0xdec0de_u64] {
        run_prepared_literal_oracle(seed);
    }
}

fn run_prepared_literal_oracle(mut seed: u64) {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let param_query = select_query(
        Select::new([
            SelectItem::expr(Expr::column("id")),
            SelectItem::expr(Expr::column("title")),
        ])
        .from([TableRef::named("albums")])
        .where_(Expr::binary(
            Expr::column("artist_id"),
            BinaryOp::Eq,
            Expr::parameter("artist"),
        )),
    );
    let prepared = database.prepare_query(param_query).unwrap();
    let artist = (seed % 4) + 1;
    let prepared_sub = database
        .bind(&prepared, &[("artist", Value::U64(artist))])
        .unwrap();
    let literal_query = literal_artist_query(artist);
    let literal_sub = database.subscribe_query(literal_query).unwrap();
    let mut prepared_rows = std::collections::BTreeMap::<(u64, String), i64>::new();
    let mut literal_rows = std::collections::BTreeMap::<(u64, String), i64>::new();
    drain_prepared_album_rows(&prepared_sub, &mut prepared_rows);
    drain_literal_album_rows(&literal_sub, &mut literal_rows);
    assert_eq!(prepared_rows, literal_rows);
    let mut known = std::collections::BTreeSet::<u64>::new();

    for step in 0..120 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let id = (seed % 24) + 1;
        let next_artist = ((seed >> 11) % 4) + 1;
        let title = format!("album-{step}-{id}");
        let mut batch = database.open_batch();
        if known.contains(&id) {
            if seed & 1 == 0 {
                batch.update(
                    "albums",
                    vec![
                        Value::U64(id),
                        Value::U64(next_artist),
                        Value::String(title),
                    ],
                );
            } else {
                known.remove(&id);
                batch.delete("albums", PrimaryKeyValue::U64(id));
            }
        } else {
            known.insert(id);
            batch.insert(
                "albums",
                vec![
                    Value::U64(id),
                    Value::U64(next_artist),
                    Value::String(title),
                ],
            );
        }
        database.commit_batch(batch).unwrap();
        drain_prepared_album_rows(&prepared_sub, &mut prepared_rows);
        drain_literal_album_rows(&literal_sub, &mut literal_rows);
        assert_eq!(
            prepared_rows, literal_rows,
            "prepared/literal mismatch after seed {seed:#x} step {step}"
        );
    }
}

fn literal_artist_query(artist: u64) -> Query {
    select_query(
        Select::new([
            SelectItem::expr(Expr::column("id")),
            SelectItem::expr(Expr::column("title")),
        ])
        .from([TableRef::named("albums")])
        .where_(Expr::binary(
            Expr::column("artist_id"),
            BinaryOp::Eq,
            Expr::Literal(Value::U64(artist)),
        )),
    )
}

fn drain_prepared_album_rows(
    subscription: &Subscription,
    rows: &mut std::collections::BTreeMap<(u64, String), i64>,
) {
    while let Ok(deltas) = subscription.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            let [Value::U64(id), Value::String(title)] = values.as_slice() else {
                panic!("unexpected prepared album row: {values:?}");
            };
            *rows.entry((*id, title.clone())).or_default() += weight;
        }
    }
    rows.retain(|_, weight| *weight != 0);
}

fn drain_literal_album_rows(
    subscription: &Subscription,
    rows: &mut std::collections::BTreeMap<(u64, String), i64>,
) {
    while let Ok(deltas) = subscription.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            let [Value::U64(id), Value::String(title)] = values.as_slice() else {
                panic!("unexpected literal album row: {values:?}");
            };
            *rows.entry((*id, title.clone())).or_default() += weight;
        }
    }
    rows.retain(|_, weight| *weight != 0);
}

#[test]
fn binding_sources_are_rejected_outside_prepared_shapes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();

    assert!(
        database
            .subscribe_one_sink(artist_album_shape_graph())
            .is_err()
    );
}

#[test]
fn duplicate_join_subscriptions_share_state_without_double_applying_deltas() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let graph = GraphBuilder::join(
        GraphBuilder::table("albums"),
        GraphBuilder::table("artists"),
        ["artist_id"],
        ["id"],
    );
    let first = database.subscribe_one_sink(graph.clone()).unwrap();
    let second = database.subscribe_one_sink(graph).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "artists",
        vec![Value::U64(11), Value::String("John Coltrane".to_owned())],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(7),
            Value::U64(11),
            Value::String("Blue Train".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&first),
        [(
            vec![
                7_u64.into(),
                11_u64.into(),
                "Blue Train".into(),
                11_u64.into(),
                "John Coltrane".into(),
            ],
            1
        )]
    );
    assert_eq!(
        expect_recv_vals(&second),
        [(
            vec![
                7_u64.into(),
                11_u64.into(),
                "Blue Train".into(),
                11_u64.into(),
                "John Coltrane".into(),
            ],
            1
        )]
    );

    assert!(database.unsubscribe(first.id()));
    assert!(database.unsubscribe(second.id()));
    assert!(database.ivm_runtime.retained_node_ids().is_empty());
}

#[test]
fn database_creation_dedups_schema_indices_as_durable_nodes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let database = Database::new(indexed_albums_schema(), storage).unwrap();

    let durable_nodes = database
        .ivm_runtime
        .retained_node_ids()
        .into_iter()
        .filter(|node| {
            database
                .ivm_runtime
                .graph()
                .node(*node)
                .is_some_and(|node| node.is_durable())
        })
        .collect::<Vec<_>>();

    assert_eq!(durable_nodes.len(), 1);
}

#[test]
fn persist_maintains_schema_index_entries() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let prefix = b"albums\0albums_by_title\0";
    let entries = database.storage.prefix("indices", prefix).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(persisted_index_value(&entries[0].1), []);

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let entries = database.storage.prefix("indices", prefix).unwrap();
    assert_eq!(entries.len(), 1);
    assert!(
        entries[0]
            .0
            .windows("Giant Steps".len())
            .any(|window| window == b"Giant Steps")
    );
    assert_eq!(persisted_index_value(&entries[0].1), []);

    let mut batch = database.open_batch();
    batch.delete("albums", PrimaryKeyValue::U64(7));
    database.commit_batch(batch).unwrap();

    assert!(
        database
            .storage
            .prefix("indices", prefix)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn persist_consolidates_same_tick_deltas_and_rejects_unique_conflicts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(unique_indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(
        record_values(
            database
                .index_scan(
                    "albums",
                    "unique_albums_by_title",
                    &[Value::String("Blue Train".to_owned())],
                )
                .unwrap()
        ),
        [vec![Value::U64(7), Value::String("Blue Train".to_owned())]]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Blue Train".to_owned())],
    );
    assert!(matches!(
        database.commit_batch(batch).unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::UniqueIndexViolation { .. })
    ));
}

#[test]
fn public_database_facade_reads_secondary_indexes_with_memory_storage() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("year", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    let storage = MemoryStorage::new(&["albums", "indices"]);
    let mut database = Database::new(schema, storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::String("Blue Train".to_owned()),
            Value::U64(1957),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(2),
            Value::String("Kind of Blue".to_owned()),
            Value::U64(1959),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(3),
            Value::String("Mingus Ah Um".to_owned()),
            Value::U64(1959),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(4),
            Value::String("A Love Supreme".to_owned()),
            Value::U64(1965),
        ],
    );
    database.commit_batch(batch).unwrap();

    let albums_from_1959 = record_values(
        database
            .index_scan("albums", "albums_by_year", &[Value::U64(1959)])
            .unwrap(),
    );
    assert_eq!(
        albums_from_1959,
        vec![
            vec![
                Value::U64(2),
                Value::String("Kind of Blue".to_owned()),
                Value::U64(1959),
            ],
            vec![
                Value::U64(3),
                Value::String("Mingus Ah Um".to_owned()),
                Value::U64(1959),
            ],
        ]
    );

    let late_1950s_and_early_1960s = record_values(
        database
            .index_scan_range(
                "albums",
                "albums_by_year",
                &[Value::U64(1959)],
                &[Value::U64(1965)],
            )
            .unwrap(),
    );
    assert_eq!(late_1950s_and_early_1960s, albums_from_1959);
}

#[test]
fn index_reads_track_insert_update_delete_and_prefixes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tracks", "indices"]).unwrap();
    let mut database = Database::new(indexed_tracks_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "tracks",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::Nullable(None),
            Value::String("Intro".to_owned()),
        ],
    );
    batch.insert(
        "tracks",
        vec![
            Value::U64(2),
            Value::U64(7),
            Value::Nullable(Some(Box::new(Value::U64(2)))),
            Value::String("Part Two".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        record_values(
            database
                .index_get(
                    "tracks",
                    "tracks_by_album_disc",
                    &[Value::U64(7), Value::Nullable(None),]
                )
                .unwrap()
        ),
        vec![vec![
            Value::U64(1),
            Value::U64(7),
            Value::Nullable(None),
            Value::String("Intro".to_owned()),
        ]]
    );
    assert_eq!(
        record_values(
            database
                .index_scan("tracks", "tracks_by_album_disc", &[Value::U64(7)])
                .unwrap()
        )
        .len(),
        2
    );

    let mut batch = database.open_batch();
    batch.update(
        "tracks",
        vec![
            Value::U64(1),
            Value::U64(8),
            Value::Nullable(None),
            Value::String("Intro".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();
    assert!(
        database
            .index_scan("tracks", "tracks_by_album_disc", &[Value::U64(7)])
            .unwrap()
            .len()
            == 1
    );

    let mut batch = database.open_batch();
    batch.delete("tracks", PrimaryKeyValue::U64(2));
    database.commit_batch(batch).unwrap();
    assert!(
        database
            .index_scan("tracks", "tracks_by_album_disc", &[Value::U64(7)])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn persisted_index_update_retracts_old_key_when_indexed_value_changes_to_finite() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "indices"]).unwrap();
    let mut database = Database::new(interval_history_schema(), storage).unwrap();
    let row = vec![7; 16];

    let mut batch = database.open_batch();
    batch.insert(
        "history",
        vec![
            Value::Bytes(row.clone()),
            Value::U64(1),
            Value::U64(1),
            Value::U64(u64::MAX),
            Value::String("open".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(
        database
            .index_scan("history", "history_by_until_row", &[Value::U64(u64::MAX)])
            .unwrap()
            .len(),
        1
    );

    let mut batch = database.open_batch();
    batch.update(
        "history",
        vec![
            Value::Bytes(row),
            Value::U64(1),
            Value::U64(1),
            Value::U64(2),
            Value::String("closed".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert!(
        database
            .index_scan("history", "history_by_until_row", &[Value::U64(u64::MAX)])
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        record_values(
            database
                .index_scan("history", "history_by_until_row", &[Value::U64(2)])
                .unwrap()
        ),
        vec![vec![
            Value::Bytes(vec![7; 16]),
            Value::U64(1),
            Value::U64(1),
            Value::U64(2),
            Value::String("closed".to_owned()),
        ]]
    );
}

#[test]
fn persisted_index_update_preserves_entry_when_index_key_is_unchanged() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "indices"]).unwrap();
    let mut database = Database::new(interval_history_schema(), storage).unwrap();
    let row = vec![7; 16];

    let mut batch = database.open_batch();
    batch.insert(
        "history",
        vec![
            Value::Bytes(row.clone()),
            Value::U64(1),
            Value::U64(1),
            Value::U64(u64::MAX),
            Value::String("before".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let mut batch = database.open_batch();
    batch.update(
        "history",
        vec![
            Value::Bytes(row),
            Value::U64(1),
            Value::U64(1),
            Value::U64(u64::MAX),
            Value::String("after".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        record_values(
            database
                .index_scan("history", "history_by_until_row", &[Value::U64(u64::MAX)])
                .unwrap()
        ),
        vec![vec![
            Value::Bytes(vec![7; 16]),
            Value::U64(1),
            Value::U64(1),
            Value::U64(u64::MAX),
            Value::String("after".to_owned()),
        ]]
    );
}

#[test]
fn uuid_primary_keys_nullable_index_keys_and_ordering_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["docs", "indices"]).unwrap();
    let mut database = Database::new(uuid_docs_schema(), storage).unwrap();
    let low = uuid::Uuid::from_bytes([1; 16]);
    let mid = uuid::Uuid::from_bytes([2; 16]);
    let high = uuid::Uuid::from_bytes([3; 16]);
    let owner = uuid::Uuid::from_bytes([9; 16]);

    let mut batch = database.open_batch();
    batch.insert(
        "docs",
        vec![
            Value::Uuid(high),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::String("high".to_owned()),
        ],
    );
    batch.insert(
        "docs",
        vec![
            Value::Uuid(low),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::String("low".to_owned()),
        ],
    );
    batch.insert(
        "docs",
        vec![
            Value::Uuid(mid),
            Value::Nullable(None),
            Value::String("mid".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        record_values(
            database
                .index_scan(
                    "docs",
                    "docs_by_owner",
                    &[Value::Nullable(Some(Box::new(Value::Uuid(owner))))],
                )
                .unwrap(),
        ),
        vec![
            vec![
                Value::Uuid(low),
                Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
                Value::String("low".to_owned()),
            ],
            vec![
                Value::Uuid(high),
                Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
                Value::String("high".to_owned()),
            ],
        ]
    );
    assert_eq!(
        database
            .index_scan("docs", "docs_by_owner", &[Value::Nullable(None)])
            .unwrap()
            .len(),
        1
    );

    let mut batch = database.open_batch();
    batch.update(
        "docs",
        vec![
            Value::Uuid(mid),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::String("mid-owned".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        database
            .index_scan(
                "docs",
                "docs_by_owner",
                &[Value::Nullable(Some(Box::new(Value::Uuid(owner))))],
            )
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn index_get_on_unique_index_returns_zero_or_one_record() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tracks", "indices"]).unwrap();
    let mut database = Database::new(indexed_tracks_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "tracks",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::Nullable(None),
            Value::String("Intro".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        database
            .index_get(
                "tracks",
                "tracks_by_title_unique",
                &[Value::String("Intro".to_owned())],
            )
            .unwrap()
            .len(),
        1
    );
    assert!(
        database
            .index_get(
                "tracks",
                "tracks_by_title_unique",
                &[Value::String("Missing".to_owned())],
            )
            .unwrap()
            .is_empty()
    );
}

#[test]
fn tuple_columns_work_in_index_keys_and_nullable_columns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "indices"]).unwrap();
    let mut database = Database::new(tuple_edges_schema(), storage).unwrap();
    let node_a = uuid::Uuid::from_bytes([0x0a; 16]);
    let node_b = uuid::Uuid::from_bytes([0x0b; 16]);
    let parent_a = Value::Tuple(vec![Value::Uuid(node_a), Value::U64(1)]);
    let parent_b = Value::Tuple(vec![Value::Uuid(node_b), Value::U64(2)]);

    let mut batch = database.open_batch();
    batch.insert(
        "edges",
        vec![
            Value::U64(1),
            parent_b.clone(),
            Value::Nullable(Some(Box::new(parent_a.clone()))),
            Value::String("b".to_owned()),
        ],
    );
    batch.insert(
        "edges",
        vec![
            Value::U64(2),
            parent_a.clone(),
            Value::Nullable(None),
            Value::String("a".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let rows = database
        .index_get("edges", "edges_by_parent", std::slice::from_ref(&parent_a))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("title").unwrap(), Value::String("a".to_owned()));

    let scanned = database
        .index_scan("edges", "edges_by_parent", &[])
        .unwrap()
        .into_iter()
        .map(|record| record.get("title").unwrap().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        scanned,
        vec![Value::String("a".to_owned()), Value::String("b".to_owned())]
    );

    let rows = database
        .index_get("edges", "edges_by_parent", &[parent_b])
        .unwrap();
    assert_eq!(
        rows[0].get("maybe_parent").unwrap(),
        Value::Nullable(Some(Box::new(parent_a)))
    );
}

#[test]
fn raw_reads_return_encoded_base_records() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tracks", "indices"]).unwrap();
    let mut database = Database::new(indexed_tracks_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    batch.insert("tracks", track_values(2, 7, None, ""));
    database.commit_batch(batch).unwrap();

    let descriptor = database
        .ivm_runtime
        .schema()
        .table("tracks")
        .unwrap()
        .record_schema();
    let title_idx = descriptor.field_index("title").unwrap();
    let album_idx = descriptor.field_index("album_id").unwrap();

    let by_pk = database
        .primary_key_scan_raw("tracks", &[Value::U64(1)])
        .unwrap();
    assert_eq!(by_pk.len(), 1);
    assert_eq!(by_pk[0].record().get_str(title_idx).unwrap(), "Intro");

    let by_index = database
        .index_scan_raw("tracks", "tracks_by_album_disc", &[Value::U64(7)])
        .unwrap();
    assert_eq!(by_index.len(), 2);
    assert_eq!(by_index[0].record().get_u64(album_idx).unwrap(), 7);

    let exact = database
        .index_get_raw(
            "tracks",
            "tracks_by_album_disc",
            &[Value::U64(7), Value::Nullable(None)],
        )
        .unwrap();
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].record().get_str(title_idx).unwrap(), "");

    let ranged = database
        .index_scan_range_raw(
            "tracks",
            "tracks_by_album_disc",
            &[Value::U64(7)],
            &[Value::U64(8)],
        )
        .unwrap();
    assert_eq!(ranged.len(), 2);
}

#[test]
fn persisted_index_scan_treats_missing_primary_key_record_as_invalid() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    database
        .storage
        .delete("albums", &PrimaryKeyValue::U64(7).into_bytes())
        .unwrap();

    assert!(matches!(
        database
            .index_scan("albums", "albums_by_title", &[Value::String("Blue Train".to_owned())])
            .unwrap_err(),
        Error::InvalidPersistedIndex(index) if index == "albums_by_title"
    ));
}

#[test]
fn primary_key_last_before_or_at_raw_returns_bounded_prefix_winner() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "indices"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "older"));
    batch.insert("history", history_values(1, 20, 1, "winner"));
    batch.insert("history", history_values(1, 30, 1, "too-new"));
    batch.insert("history", history_values(2, 15, 1, "other-row"));
    database.commit_batch(batch).unwrap();

    let descriptor = database
        .ivm_runtime
        .schema()
        .table("history")
        .unwrap()
        .record_schema();
    let title_idx = descriptor.field_index("title").unwrap();
    let bounded = database
        .primary_key_last_before_or_at_raw(
            "history",
            &[Value::U64(1)],
            &[Value::U64(1), Value::U64(20), Value::U64(u64::MAX)],
        )
        .unwrap()
        .expect("bounded row");
    assert_eq!(bounded.record().get_str(title_idx).unwrap(), "winner");

    let before_first = database
        .primary_key_last_before_or_at_raw(
            "history",
            &[Value::U64(1)],
            &[Value::U64(1), Value::U64(5), Value::U64(u64::MAX)],
        )
        .unwrap();
    assert!(before_first.is_none());

    let ranged = database
        .primary_key_scan_range_raw(
            "history",
            &[Value::U64(1), Value::U64(10), Value::U64(0)],
            &[Value::U64(1), Value::U64(30), Value::U64(0)],
        )
        .unwrap();
    let titles = ranged
        .iter()
        .map(|raw| raw.record().get_str(title_idx).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(titles, vec!["older", "winner"]);
}

#[test]
fn randomized_index_reads_match_full_scan_oracle() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tracks", "indices"]).unwrap();
    let mut database = Database::new(indexed_tracks_schema(), storage).unwrap();
    let mut rows = std::collections::BTreeMap::<u64, (u64, Option<u64>, String)>::new();
    let mut rng = 0x51eed_u64;

    for _ in 0..200 {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = (rng % 24) + 1;
        let album = ((rng >> 8) % 5) + 1;
        let disc = (!(rng >> 16).is_multiple_of(3)).then_some(((rng >> 24) % 3) + 1);
        let title = format!("t{id}-{album}-{}", disc.unwrap_or(0));
        let mut batch = database.open_batch();
        if rng & 1 == 0 || !rows.contains_key(&id) {
            rows.insert(id, (album, disc, title.clone()));
            batch.update("tracks", track_values(id, album, disc, &title));
        } else {
            rows.remove(&id);
            batch.delete("tracks", PrimaryKeyValue::U64(id));
        }
        database.commit_batch(batch).unwrap();

        let album_key = Value::U64(album);
        let mut expected = rows
            .iter()
            .filter(|(_, (row_album, _, _))| *row_album == album)
            .map(|(row_id, (row_album, row_disc, row_title))| {
                track_values(*row_id, *row_album, *row_disc, row_title)
            })
            .collect::<Vec<_>>();
        expected.sort_by_key(|values| format!("{values:?}"));
        let mut actual = record_values(
            database
                .index_scan("tracks", "tracks_by_album_disc", &[album_key])
                .unwrap(),
        );
        actual.sort_by_key(|values| format!("{values:?}"));
        assert_eq!(actual, expected);
    }
}

#[test]
fn persisted_index_keys_sort_by_index_value_then_primary_key() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(256), Value::String("b".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("aa".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let keys = database
        .storage
        .prefix("indices", b"albums\0albums_by_title\0")
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .collect::<Vec<_>>();

    assert_eq!(
        keys,
        [
            persisted_index_storage_key("albums_by_title", &encoded_title_index_key("aa", 1)),
            persisted_index_storage_key("albums_by_title", &encoded_title_index_key("b", 256)),
        ]
    );
}

#[test]
fn durable_non_unique_index_keys_append_separator_and_primary_key_suffix() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let entries = database
        .storage
        .prefix("indices", b"albums\0albums_by_title\0")
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].0,
        persisted_index_storage_key("albums_by_title", &encoded_title_index_key("Blue Train", 7))
    );
    assert!(
        encoded_title_index_key("Blue Train", 7)
            .strip_prefix(encoded_title_key_part("Blue Train").as_slice())
            .is_some_and(|suffix| suffix.starts_with(&[0xff]))
    );
}

#[test]
fn unique_indices_use_only_index_columns_as_storage_keys() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(unique_indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let prefix = b"albums\0unique_albums_by_title\0";
    let entries = database.storage.prefix("indices", prefix).unwrap();
    let expected_key = persisted_index_storage_key(
        "unique_albums_by_title",
        &encoded_title_key_part("Blue Train"),
    );

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, expected_key);
    assert_eq!(
        persisted_index_value(&entries[0].1),
        encoded_u64_index_part(7)
    );
}

#[test]
fn durable_unique_index_keys_omit_primary_key_suffix() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(unique_indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let entries = database
        .storage
        .prefix("indices", b"albums\0unique_albums_by_title\0")
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].0,
        persisted_index_storage_key(
            "unique_albums_by_title",
            &encoded_title_key_part("Blue Train"),
        )
    );
    assert!(!entries[0].0.ends_with(&encoded_u64_index_part(7)));
}

#[test]
fn primary_key_covering_indices_omit_redundant_suffix_and_recover_pk_from_key() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "history",
        [
            ColumnSchema::new("row", ColumnType::U64),
            ColumnSchema::new("stamp", ColumnType::U64),
            ColumnSchema::new("node", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("row", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("stamp", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("node", IntegerKeyType::U64),
    ]))
    .with_index(IndexSchema::new("by_tx", ["stamp", "node", "row"]))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "indices"]).unwrap();
    let mut database = Database::new(schema, storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(2, 10, 1, "older"));
    batch.insert("history", history_values(1, 20, 7, "newer"));
    database.commit_batch(batch).unwrap();

    let entries = database
        .storage
        .prefix("indices", b"history\0by_tx\0")
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.0.clone())
            .collect::<Vec<_>>(),
        [
            persisted_table_index_storage_key(
                "history",
                "by_tx",
                &encoded_history_by_tx_key(10, 1, 2)
            ),
            persisted_table_index_storage_key(
                "history",
                "by_tx",
                &encoded_history_by_tx_key(20, 7, 1)
            ),
        ]
    );
    assert!(
        entries
            .iter()
            .all(|(_, record)| persisted_index_value(record).is_empty())
    );

    let latest = database
        .index_last_raw("history", "by_tx", &[])
        .unwrap()
        .unwrap();
    assert_eq!(latest.key(), &history_key(1, 20, 7).into_bytes());
    assert_eq!(
        latest.record().get("title").unwrap(),
        Value::String("newer".to_owned())
    );

    let stamp_scan = database
        .index_scan("history", "by_tx", &[Value::U64(10)])
        .unwrap();
    assert_eq!(
        record_values(stamp_scan),
        [history_values(2, 10, 1, "older")]
    );
}

#[test]
fn unique_indices_reject_existing_conflicting_values() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(unique_indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Blue Train".to_owned())],
    );
    assert!(matches!(
        database.commit_batch(batch).unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::UniqueIndexViolation { .. })
    ));

    let prefix = b"albums\0unique_albums_by_title\0";
    let entries = database.storage.prefix("indices", prefix).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        persisted_index_value(&entries[0].1),
        encoded_u64_index_part(7)
    );
}

#[test]
fn durable_unique_indices_reject_positive_delta_for_existing_different_record() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(unique_indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Blue Train".to_owned())],
    );

    assert!(matches!(
        database.commit_batch(batch).unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::UniqueIndexViolation { .. })
    ));
}

#[test]
fn unique_indices_reject_conflicts_within_one_batch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
    let mut database = Database::new(unique_indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Blue Train".to_owned())],
    );

    assert!(matches!(
        database.commit_batch(batch).unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::UniqueIndexViolation { .. })
    ));
    assert!(
        database
            .storage
            .prefix("indices", b"albums\0unique_albums_by_title\0")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn table_and_index_state_survive_restart_for_resubscribed_graphs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let table_graph = GraphBuilder::table("albums");
    let index_graph = GraphBuilder::index("albums", "albums_by_title");

    {
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
        let mut database = Database::new(indexed_albums_schema(), storage).unwrap();
        database.subscribe_one_sink(table_graph.clone()).unwrap();
        database.subscribe_one_sink(index_graph.clone()).unwrap();

        let mut batch = database.open_batch();
        batch.insert(
            "albums",
            vec![Value::U64(7), Value::String("Blue Train".to_owned())],
        );
        database.commit_batch(batch).unwrap();
    }

    {
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
        let mut database = Database::new(indexed_albums_schema(), storage).unwrap();
        let table_subscription_id = database.subscribe_one_sink(table_graph).unwrap();
        let index_subscription_id = database.subscribe_one_sink(index_graph).unwrap();

        database.flush().unwrap();
        assert_eq!(
            expect_recv_vals(&table_subscription_id),
            [(vec![7_u64.into(), "Blue Train".into()], 1)]
        );
        assert_eq!(
            expect_recv_vals(&index_subscription_id),
            [(
                vec![
                    encoded_title_index_key("Blue Train", 7).into(),
                    Vec::<u8>::new().into(),
                ],
                1,
            )]
        );

        let mut batch = database.open_batch();
        batch.update(
            "albums",
            vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
        );
        database.commit_batch(batch).unwrap();

        assert_eq!(
            expect_recv_vals(&table_subscription_id),
            [
                (vec![7_u64.into(), "Blue Train".into()], -1),
                (vec![7_u64.into(), "Giant Steps".into()], 1),
            ]
        );

        assert_eq!(
            expect_recv_vals(&index_subscription_id),
            [
                (
                    vec![
                        encoded_title_index_key("Blue Train", 7).into(),
                        Vec::<u8>::new().into(),
                    ],
                    -1,
                ),
                (
                    vec![
                        encoded_title_index_key("Giant Steps", 7).into(),
                        Vec::<u8>::new().into(),
                    ],
                    1,
                ),
            ]
        );
    }
}

#[test]
fn persisted_indices_can_be_deleted_after_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let table_graph = GraphBuilder::table("albums");
    let index_graph = GraphBuilder::index("albums", "albums_by_title");

    {
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
        let mut database = Database::new(indexed_albums_schema(), storage).unwrap();
        database.subscribe_one_sink(table_graph.clone()).unwrap();
        database.subscribe_one_sink(index_graph.clone()).unwrap();

        let mut batch = database.open_batch();
        batch.insert(
            "albums",
            vec![Value::U64(7), Value::String("Blue Train".to_owned())],
        );
        database.commit_batch(batch).unwrap();
    }

    {
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "indices"]).unwrap();
        let mut database = Database::new(indexed_albums_schema(), storage).unwrap();
        let table_subscription_id = database.subscribe_one_sink(table_graph).unwrap();
        let index_subscription_id = database.subscribe_one_sink(index_graph).unwrap();

        database.flush().unwrap();
        assert_eq!(
            expect_recv_vals(&table_subscription_id),
            [(vec![7_u64.into(), "Blue Train".into()], 1)]
        );
        assert_eq!(
            expect_recv_vals(&index_subscription_id),
            [(
                vec![
                    encoded_title_index_key("Blue Train", 7).into(),
                    Vec::<u8>::new().into(),
                ],
                1,
            )]
        );

        let mut batch = database.open_batch();
        batch.delete("albums", PrimaryKeyValue::U64(7));
        database.commit_batch(batch).unwrap();

        assert_eq!(
            expect_recv_vals(&table_subscription_id),
            [(vec![7_u64.into(), "Blue Train".into()], -1)]
        );
        assert_eq!(
            expect_recv_vals(&index_subscription_id),
            [(
                vec![
                    encoded_title_index_key("Blue Train", 7).into(),
                    Vec::<u8>::new().into(),
                ],
                -1,
            )]
        );
    }
}

#[test]
fn query_subscription_matches_one_shot_recompute_under_seeded_interleavings() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let query = select_query(
        Select::new([SelectItem::expr(col("title"))])
            .from([TableRef::named("albums")])
            .where_(Expr::binary(
                col("id"),
                BinaryOp::Gt,
                Expr::Literal(Value::U64(10)),
            )),
    );
    let subscription = database.subscribe_query(query.clone()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut seed = 0x5eed_u64;
    let mut known = std::collections::HashSet::<u64>::new();
    let mut materialized = std::collections::BTreeMap::<String, i64>::new();

    for step in 0..96 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = (seed % 20) + 1;
        let action = (seed >> 8) % 3;
        let mut batch = database.open_batch();
        match (action, known.contains(&id)) {
            (0, false) => {
                known.insert(id);
                batch.insert(
                    "albums",
                    vec![Value::U64(id), Value::String(format!("a{step}"))],
                );
            }
            (1, true) => {
                batch.update(
                    "albums",
                    vec![Value::U64(id), Value::String(format!("u{step}"))],
                );
            }
            (_, true) => {
                known.remove(&id);
                batch.delete("albums", PrimaryKeyValue::U64(id));
            }
            _ => continue,
        }
        database.commit_batch(batch).unwrap();

        while let Ok(deltas) = subscription.try_recv() {
            for (values, weight) in deltas.to_values().unwrap() {
                let [Value::String(title)] = values.as_slice() else {
                    panic!("expected projected title, got {values:?}");
                };
                *materialized.entry(title.clone()).or_default() += weight;
            }
            materialized.retain(|_, weight| *weight != 0);
        }

        let recomputed = database.query(query.clone()).unwrap();
        let mut expected = std::collections::BTreeMap::<String, i64>::new();
        for (values, weight) in recomputed.to_values().unwrap() {
            let [Value::String(title)] = values.as_slice() else {
                panic!("expected projected title, got {values:?}");
            };
            *expected.entry(title.clone()).or_default() += weight;
        }
        expected.retain(|_, weight| *weight != 0);
        assert_eq!(
            materialized, expected,
            "mismatch after generated step {step}"
        );
    }
}

struct FamilyOracleSubscription {
    param: u64,
    subscription: Subscription,
    materialized: std::collections::BTreeMap<(u64, u64, String), i64>,
}

impl FamilyOracleSubscription {
    fn new(param: u64, subscription: Subscription) -> Self {
        Self {
            param,
            subscription,
            materialized: std::collections::BTreeMap::new(),
        }
    }

    fn drain(&mut self) {
        while let Ok(deltas) = self.subscription.try_recv() {
            apply_artist_album_deltas(&mut self.materialized, deltas);
        }
    }
}

#[test]
fn shape_subscriptions_match_recompute_under_seeded_interleavings() {
    for seed in [0xfade_u64, 0xbad5eed_u64, 0x51a7e_u64, 0xaced_u64] {
        run_shape_subscription_oracle(seed);
    }
}

fn run_shape_subscription_oracle(mut seed: u64) {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let shape = database
        .prepare_one_sink(
            artist_album_shape_graph(),
            "artist_params",
            artist_binding_descriptor(),
            ["artist_id"],
        )
        .unwrap();
    let mut albums = std::collections::BTreeMap::<u64, (u64, String)>::new();
    let mut subscriptions = Vec::<FamilyOracleSubscription>::new();

    for step in 0..160 {
        seed = seed
            .wrapping_mul(3202034522624059733)
            .wrapping_add(4354685564936845355);
        match (seed >> 9) % 8 {
            0 | 1 => {
                let param = ((seed >> 18) % 6) + 1;
                let mut subscription = FamilyOracleSubscription::new(
                    param,
                    database
                        .bind_shape_one_sink(shape.id(), &[Value::U64(param)])
                        .unwrap(),
                );
                subscription.drain();
                assert_shape_subscription_matches_oracle(&subscription, &albums, seed, step);
                subscriptions.push(subscription);
            }
            2 if !subscriptions.is_empty() => {
                let idx = (seed as usize) % subscriptions.len();
                let subscription = subscriptions.swap_remove(idx);
                assert!(database.unsubscribe(subscription.subscription.id()));
            }
            3 if !subscriptions.is_empty() => {
                let idx = (seed as usize) % subscriptions.len();
                drop(subscriptions.swap_remove(idx));
            }
            _ => {
                let id = (seed % 32) + 1;
                let artist = ((seed >> 21) % 6) + 1;
                let title = format!("album-{step}-{id}");
                let mut batch = database.open_batch();
                match albums.entry(id) {
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        if seed & 1 == 0 {
                            entry.insert((artist, title.clone()));
                            batch.update(
                                "albums",
                                vec![Value::U64(id), Value::U64(artist), Value::String(title)],
                            );
                        } else {
                            entry.remove();
                            batch.delete("albums", PrimaryKeyValue::U64(id));
                        }
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert((artist, title.clone()));
                        batch.insert(
                            "albums",
                            vec![Value::U64(id), Value::U64(artist), Value::String(title)],
                        );
                    }
                }
                database.commit_batch(batch).unwrap();
                for subscription in &mut subscriptions {
                    subscription.drain();
                    assert_shape_subscription_matches_oracle(subscription, &albums, seed, step);
                }
            }
        }
    }
}

fn apply_artist_album_deltas(
    materialized: &mut std::collections::BTreeMap<(u64, u64, String), i64>,
    deltas: RecordDeltas,
) {
    for (values, weight) in deltas.to_values().unwrap() {
        let [
            Value::U64(artist_id),
            Value::U64(album_id),
            Value::String(title),
        ] = values.as_slice()
        else {
            panic!("expected artist album delta, got {values:?}");
        };
        *materialized
            .entry((*artist_id, *album_id, title.clone()))
            .or_default() += weight;
    }
    materialized.retain(|_, weight| *weight != 0);
}

fn assert_shape_subscription_matches_oracle(
    subscription: &FamilyOracleSubscription,
    albums: &std::collections::BTreeMap<u64, (u64, String)>,
    seed: u64,
    step: usize,
) {
    let expected = albums
        .iter()
        .filter(|(_, (artist_id, _))| *artist_id == subscription.param)
        .map(|(album_id, (artist_id, title))| ((*artist_id, *album_id, title.clone()), 1))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        subscription.materialized, expected,
        "shape subscription mismatch after generated seed {seed:#x} step {step}"
    );
}

#[derive(Clone, Copy, Debug)]
enum OracleGraph {
    Reach,
    TwoHop,
    UnblockedEdges,
}

struct OracleSubscription {
    graph: OracleGraph,
    subscription: Subscription,
    materialized: std::collections::BTreeMap<(u64, u64), i64>,
    created_step: usize,
}

impl OracleSubscription {
    fn new(graph: OracleGraph, subscription: Subscription, created_step: usize) -> Self {
        Self {
            graph,
            subscription,
            materialized: std::collections::BTreeMap::new(),
            created_step,
        }
    }

    fn drain(&mut self) {
        while let Ok(deltas) = self.subscription.try_recv() {
            apply_pair_deltas(&mut self.materialized, deltas);
        }
    }
}

#[test]
fn graph_subscriptions_match_recompute_under_seeded_interleavings() {
    for seed in [0xc0ffee_u64, 0x5eed_u64, 0xfacefeed_u64, 0xdecafbad_u64] {
        run_graph_subscription_oracle(seed);
    }
}

fn run_graph_subscription_oracle(mut seed: u64) {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges", "blockers"]).unwrap();
    let mut database = Database::new(edges_blockers_schema(), storage).unwrap();
    let mut edges = std::collections::BTreeMap::<u64, (u64, u64)>::new();
    let mut blockers = std::collections::BTreeMap::<u64, (u64, u64)>::new();
    let mut subscriptions = Vec::<OracleSubscription>::new();

    for step in 0..140 {
        seed = seed
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        match (seed >> 7) % 7 {
            0 | 1 => {
                let graph = match seed % 3 {
                    0 => OracleGraph::Reach,
                    1 => OracleGraph::TwoHop,
                    _ => OracleGraph::UnblockedEdges,
                };
                let builder = match graph {
                    OracleGraph::Reach => reachability_graph(256),
                    OracleGraph::TwoHop => two_hop_graph(),
                    OracleGraph::UnblockedEdges => unblocked_edges_graph(),
                };
                let mut subscription = OracleSubscription::new(
                    graph,
                    database.subscribe_one_sink(builder).unwrap(),
                    step,
                );
                subscription.drain();
                assert_eq!(table_pairs_from_query(&mut database, "edges"), edges);
                assert_eq!(table_pairs_from_query(&mut database, "blockers"), blockers);
                assert_subscription_matches_oracle(&subscription, &edges, &blockers, seed, step);
                subscriptions.push(subscription);
            }
            2 if !subscriptions.is_empty() => {
                let idx = (seed as usize) % subscriptions.len();
                let subscription = subscriptions.swap_remove(idx);
                assert!(database.unsubscribe(subscription.subscription.id()));
            }
            3 => {
                let result = database
                    .query(select_query(
                        Select::new([SelectItem::expr(col("src")), SelectItem::expr(col("dst"))])
                            .from([TableRef::named("edges")]),
                    ))
                    .unwrap();
                assert_eq!(pairs_from_deltas(result), direct_edge_multiset(&edges));
            }
            _ => {
                let mut batch = database.open_batch();
                let mutate_edges = seed & 0b100 == 0;
                if mutate_edges {
                    mutate_pair_table(&mut batch, "edges", &mut edges, seed, step);
                } else {
                    mutate_pair_table(&mut batch, "blockers", &mut blockers, seed, step);
                }
                if mutate_edges && seed & 0b100000 == 0 {
                    mutate_pair_table(
                        &mut batch,
                        "blockers",
                        &mut blockers,
                        seed.rotate_left(17),
                        step,
                    );
                }
                database.commit_batch(batch).unwrap();
                for subscription in &mut subscriptions {
                    subscription.drain();
                    assert_subscription_matches_oracle(subscription, &edges, &blockers, seed, step);
                }
            }
        }
    }
}

fn table_pairs_from_query(
    database: &mut Database<RocksDbStorage>,
    table: &str,
) -> std::collections::BTreeMap<u64, (u64, u64)> {
    let result = database
        .query(select_query(
            Select::new([
                SelectItem::expr(col("id")),
                SelectItem::expr(col("src")),
                SelectItem::expr(col("dst")),
            ])
            .from([TableRef::named(table)]),
        ))
        .unwrap();
    result
        .to_values()
        .unwrap()
        .into_iter()
        .filter_map(|(values, weight)| {
            if weight <= 0 {
                return None;
            }
            let [Value::U64(id), Value::U64(src), Value::U64(dst)] = values.as_slice() else {
                panic!("expected id/src/dst row, got {values:?}");
            };
            Some((*id, (*src, *dst)))
        })
        .collect()
}

fn record_values(records: Vec<Record<'_>>) -> Vec<Vec<Value>> {
    records
        .into_iter()
        .map(|record| record.to_values().unwrap())
        .collect()
}

fn mutate_pair_table(
    batch: &mut DatabaseBatch,
    table: &str,
    rows: &mut std::collections::BTreeMap<u64, (u64, u64)>,
    seed: u64,
    _step: usize,
) {
    let id = (seed % 24) + 1;
    let src = ((seed >> 12) % 8) + 1;
    let dst = ((seed >> 20) % 8) + 1;
    match rows.entry(id) {
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            if seed & 1 == 0 {
                entry.insert((src, dst));
                batch.update(
                    table,
                    vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
                );
            } else {
                entry.remove();
                batch.delete(table, PrimaryKeyValue::U64(id));
            }
        }
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert((src, dst));
            batch.insert(
                table,
                vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
            );
        }
    }
}

fn apply_pair_deltas(
    materialized: &mut std::collections::BTreeMap<(u64, u64), i64>,
    deltas: RecordDeltas,
) {
    for (values, weight) in deltas.to_values().unwrap() {
        let [Value::U64(src), Value::U64(dst)] = values.as_slice() else {
            panic!("expected pair delta, got {values:?}");
        };
        *materialized.entry((*src, *dst)).or_default() += weight;
    }
    materialized.retain(|_, weight| *weight != 0);
}

fn assert_subscription_matches_oracle(
    subscription: &OracleSubscription,
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
    blockers: &std::collections::BTreeMap<u64, (u64, u64)>,
    seed: u64,
    step: usize,
) {
    let expected = match subscription.graph {
        OracleGraph::Reach => transitive_closure(edges),
        OracleGraph::TwoHop => two_hop_pairs(edges),
        OracleGraph::UnblockedEdges => unblocked_edges(edges, blockers),
    };
    assert_eq!(
        subscription.materialized, expected,
        "subscription mismatch for {:?} created at step {} after generated graph seed {seed:#x} step {step}; edges={edges:?}; blockers={blockers:?}",
        subscription.graph, subscription.created_step
    );
}

fn unblocked_edges(
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
    blockers: &std::collections::BTreeMap<u64, (u64, u64)>,
) -> std::collections::BTreeMap<(u64, u64), i64> {
    let blocker_counts = direct_edge_multiset(blockers);
    let mut pairs = std::collections::BTreeMap::new();
    for edge in edges.values() {
        if blocker_counts.get(edge).copied().unwrap_or_default() == 0 {
            *pairs.entry(*edge).or_default() += 1;
        }
    }
    pairs
}

fn direct_edges(
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
) -> std::collections::BTreeMap<(u64, u64), i64> {
    edges
        .values()
        .map(|edge| (*edge, 1))
        .collect::<std::collections::BTreeMap<_, _>>()
}

fn direct_edge_multiset(
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
) -> std::collections::BTreeMap<(u64, u64), i64> {
    let mut pairs = std::collections::BTreeMap::new();
    for edge in edges.values() {
        *pairs.entry(*edge).or_default() += 1;
    }
    pairs
}

fn pairs_from_deltas(deltas: RecordDeltas) -> std::collections::BTreeMap<(u64, u64), i64> {
    let mut pairs = std::collections::BTreeMap::new();
    apply_pair_deltas(&mut pairs, deltas);
    pairs
}

fn two_hop_pairs(
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
) -> std::collections::BTreeMap<(u64, u64), i64> {
    let mut pairs = std::collections::BTreeMap::new();
    for (left_src, left_dst) in edges.values() {
        for (right_src, right_dst) in edges.values() {
            if left_dst == right_src {
                *pairs.entry((*left_src, *right_dst)).or_default() += 1;
            }
        }
    }
    pairs.retain(|_, weight| *weight != 0);
    pairs
}

fn transitive_closure(
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
) -> std::collections::BTreeMap<(u64, u64), i64> {
    let mut closure = direct_edges(edges);
    let mut changed = true;
    while changed {
        changed = false;
        let known = closure.keys().copied().collect::<Vec<_>>();
        for (src, mid) in &known {
            for (edge_src, edge_dst) in edges.values() {
                if mid == edge_src && !closure.contains_key(&(*src, *edge_dst)) {
                    closure.insert((*src, *edge_dst), 1);
                    changed = true;
                }
            }
        }
    }
    closure
}

fn encoded_title_index_key(title: &str, primary_key: u64) -> Vec<u8> {
    let mut bytes = encoded_title_key_part(title);
    bytes.push(0xff);
    bytes.extend(encoded_u64_index_part(primary_key));
    bytes
}

fn encoded_uuid_index_key(value: uuid::Uuid, primary_key: u64) -> Vec<u8> {
    let mut bytes = vec![10];
    bytes.extend(value.as_bytes());
    bytes.push(0xff);
    bytes.extend(encoded_u64_index_part(primary_key));
    bytes
}

fn encoded_history_by_tx_key(stamp: u64, node: u64, row: u64) -> Vec<u8> {
    let mut bytes = encoded_u64_index_part(stamp);
    bytes.extend(encoded_u64_index_part(node));
    bytes.extend(encoded_u64_index_part(row));
    bytes
}

fn encoded_title_key_part(title: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(6);
    encode_ordered_bytes(&mut bytes, title.as_bytes());
    bytes
}

fn persisted_index_storage_key(index: &str, logical_key: &[u8]) -> Vec<u8> {
    persisted_table_index_storage_key("albums", index, logical_key)
}

fn persisted_table_index_storage_key(table: &str, index: &str, logical_key: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(table.as_bytes());
    bytes.push(0);
    bytes.extend(index.as_bytes());
    bytes.push(0);
    bytes.extend(encoded_bytes_key_part(logical_key));
    bytes
}

fn encoded_bytes_key_part(value: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(7);
    encode_ordered_bytes(&mut bytes, value);
    bytes
}

fn encoded_u64_index_part(value: u64) -> Vec<u8> {
    let mut bytes = vec![3];
    bytes.extend(value.to_be_bytes());
    bytes
}

fn encode_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.extend([0, 0]);
}

fn persisted_index_value(record: &[u8]) -> Vec<u8> {
    let descriptor = RecordDescriptor::new([
        ("key", crate::records::ValueType::Bytes),
        ("value", crate::records::ValueType::Bytes),
    ]);
    match descriptor.get(record, "value").unwrap() {
        Value::Bytes(value) => value,
        value => panic!("expected persisted index value bytes, got {value:?}"),
    }
}
