//! End-to-end behavior guards for the database facade and IVM integration.
//!
//! These tests own broad public-surface coverage: commits, queries,
//! subscriptions, joins, recursion, indices, prepared shapes, and persistence
//! through the [`super::Database`] API. Lower-level record-layout tests live in
//! [`crate::records::tests`]; runtime-specific regression tests live near the
//! runtime module.

use super::*;
use std::sync::mpsc::TryRecvError;

use crate::ivm::{PredicateExpr, ProjectField, TopByOrder};
use crate::queries::{
    BinaryOp, ColumnRef, Cte, Expr, JoinConstraint, JoinKind, Query, Select, SelectItem, TableRef,
    UnaryOp, WithQuery,
};
use crate::records::{EnumSchema, RecordDescriptor};
use crate::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, DirectRecordStoreSchema, IndexSchema, IntegerKeyType,
    PrimaryKey, PrimaryKeyColumn,
};
use crate::storage::{MemoryStorage, RocksDbStorage};

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

fn history_top_by_stamp_asc(limit: usize) -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("history"),
        ["row"],
        [TopByOrder::asc("stamp")],
        ["node"],
        0,
        limit,
    )
}

fn history_top_by_stamp_desc(limit: usize) -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("history"),
        ["row"],
        [TopByOrder::desc("stamp")],
        ["node"],
        0,
        limit,
    )
}

fn history_top_by_stamp_asc_offset(offset: usize, limit: usize) -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("history"),
        ["row"],
        [TopByOrder::asc("stamp")],
        ["node"],
        offset,
        limit,
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
        .prepare(
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
        .prepare(
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
    let subscription = database.subscribe(GraphBuilder::table("albums")).unwrap();
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
    let subscription = database.subscribe(GraphBuilder::table("albums")).unwrap();
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
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage).unwrap();
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

    let writes = database.last_commit_metrics().unwrap().storage_writes;
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
}

#[test]
fn subscribe_sends_empty_hydration_snapshot_without_writes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription_id = database.subscribe(GraphBuilder::table("albums")).unwrap();

    assert!(subscription_id.try_recv().unwrap().is_empty());
    database.flush().unwrap();
    assert!(subscription_id.try_recv().is_err());
    assert!(database.storage.prefix("albums", b"").unwrap().is_empty());
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
        Err(Error::Storage(crate::storage::Error::ColumnFamilyNotFound(cf))) if cf == "indices"
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
        .subscribe(GraphBuilder::join(
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
    let subscription = recursive.subscribe(reachability_graph(16)).unwrap();
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
        database.subscribe(GraphBuilder::table("missing")),
        Err(Error::IvmRuntime(IvmRuntimeError::TableNotFound(table))) if table == "missing"
    ));
    assert!(matches!(
        database.subscribe(GraphBuilder::index("albums", "missing_idx")),
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
    let subscription_id = database.subscribe(GraphBuilder::table("albums")).unwrap();

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
    let subscription = database.subscribe(GraphBuilder::table("albums")).unwrap();
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

    let subscription = database.subscribe(GraphBuilder::table("albums")).unwrap();
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
        .subscribe(
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
        .prepare(graph, "needle_param", binding_descriptor, ["needle"])
        .unwrap();
    let subscription = database
        .bind_shape(shape.id(), &[Value::String("Train".to_owned())])
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
fn unwrap_nullable_retractions_flow_symmetrically() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["tracks", "indices"]).unwrap();
    let mut database = Database::new(indexed_tracks_schema(), storage).unwrap();
    let subscription = database
        .subscribe(
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
fn arg_max_by_hydrates_and_tracks_winner_changes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "old"));
    batch.insert("history", history_values(1, 20, 1, "winner"));
    batch.insert("history", history_values(2, 5, 1, "other"));
    database.commit_batch(batch).unwrap();

    let subscription = database.subscribe(history_arg_max()).unwrap();
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
    let subscription = database.subscribe(history_arg_max()).unwrap();
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
    let subscription = database.subscribe(history_arg_max()).unwrap();
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

    let subscription = database.subscribe(history_arg_min()).unwrap();
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
    let subscription = database.subscribe(history_arg_min()).unwrap();
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
    let subscription = database.subscribe(history_arg_min()).unwrap();
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

    let subscription = database.subscribe(history_top_by_stamp_asc(2)).unwrap();
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
fn top_by_boundary_insert_and_delete_updates_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database.subscribe(history_top_by_stamp_asc(2)).unwrap();
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
    let subscription = database.subscribe(history_top_by_stamp_asc(2)).unwrap();
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

    let subscription = database.subscribe(history_top_by_stamp_desc(2)).unwrap();
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
        .subscribe(history_top_by_stamp_asc_offset(1, 1))
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
        .subscribe(GraphBuilder::top_by(
            GraphBuilder::table("scores"),
            std::iter::empty::<&str>(),
            [TopByOrder::asc("score")],
            ["id"],
            0,
            1,
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
    let subscription = database.subscribe(history_top_by_stamp_asc(1)).unwrap();
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

#[test]
fn arg_max_by_feeds_join_and_anti_join() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["history", "rows", "blockers"]).unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let visible = database
        .subscribe(GraphBuilder::anti_join(
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
        .prepare(
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
    let sub = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
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
    let subscription = database.subscribe(graph.clone()).unwrap();
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
    let subscription = database.subscribe(graph.clone()).unwrap();
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
        .subscribe(GraphBuilder::arg_max_by(
            GraphBuilder::table("history"),
            ["row"],
            ["node", "stamp"],
        ))
        .unwrap_err();
    assert!(format!("{err}").contains("requires primary key"));

    let err = database
        .subscribe(GraphBuilder::recursive(
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
        .unwrap_err();
    assert!(format!("{err}").contains("inside recursive graphs"));
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
        .prepare(
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
    let disc_one = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
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
        .prepare(
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
    let disc_one = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
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
        .prepare(
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
        .bind_shape(shape.id(), &[Value::Uuid(owner)])
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
fn subscribe_supports_recursive_hydration_snapshot_message() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).unwrap();

    let subscription = database.subscribe(reachability_graph(16)).unwrap();
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
    let subscription_id = database.subscribe(GraphBuilder::table("albums")).unwrap();

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
    database.subscribe(GraphBuilder::table("albums")).unwrap();
    database
        .subscribe(GraphBuilder::index("albums", "albums_by_title"))
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
    let subscription = database.subscribe(GraphBuilder::table("albums")).unwrap();
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
    let subscription_id = database.subscribe(reachability_graph(16)).unwrap();

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
    let subscription_id = database.subscribe(reachability_graph(16)).unwrap();

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
    let subscription = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
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
fn prepared_recursive_binding_retracts_paths_after_first_edge_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
    let mut database = Database::new(edges_schema(), storage).unwrap();
    let shape = prepared_reachability_shape(&mut database);
    let subscription = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
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
    let first = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
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

    let second = database.bind_shape(shape.id(), &[Value::U64(9)]).unwrap();
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

    let third = database.bind_shape(shape.id(), &[Value::U64(5)]).unwrap();
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
    let subscription = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
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
    let subscription = database.bind_shape(shape.id(), &[Value::U64(1)]).unwrap();
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
    let subscription_id = database.subscribe(reachability_graph(16)).unwrap();

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
    let subscription_id = database.subscribe(reachability_graph(16)).unwrap();

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
    let subscription_id = database.subscribe(reachability_graph(16)).unwrap();

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
    let subscription_id = database.subscribe(reachability_graph(16)).unwrap();

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
    let subscription = database.subscribe(reachability_graph(2)).unwrap();
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
        database.subscribe(graph).unwrap_err(),
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
        database.subscribe(graph).unwrap_err(),
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

    let first = database.subscribe(GraphBuilder::table("albums")).unwrap();
    let second = database.subscribe(GraphBuilder::table("albums")).unwrap();
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
        .subscribe(GraphBuilder::union([
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
        .subscribe(GraphBuilder::union([album_titles.clone(), album_titles]))
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
        .subscribe(GraphBuilder::table("albums").filter(PredicateExpr::gt("id", Value::U64(10))))
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
        .subscribe(GraphBuilder::table("albums").project(["title"]))
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

    let first = database.subscribe(graph.clone()).unwrap();
    let second = database.subscribe(graph).unwrap();
    let first_output = database
        .ivm_runtime
        .subscription_output_node(first.id())
        .unwrap();
    let second_output = database
        .ivm_runtime
        .subscription_output_node(second.id())
        .unwrap();

    assert_eq!(first_output, second_output);
    assert_eq!(database.ivm_runtime.retained_node_ids().len(), 4);

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
        .subscribe(GraphBuilder::join(
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
        .subscribe(GraphBuilder::join(
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
        .subscribe(GraphBuilder::join(
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
        .subscribe(GraphBuilder::anti_join(
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
fn anti_join_retracts_and_restores_on_right_threshold_transitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();
    let subscription = database
        .subscribe(GraphBuilder::anti_join(
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
        .subscribe(GraphBuilder::anti_join(
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
        .subscribe(GraphBuilder::anti_join(
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

    let subscription = database.subscribe(unblocked_edges_graph()).unwrap();
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

    let subscription = database.subscribe(unblocked_edges_graph()).unwrap();
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

    let subscription = database.subscribe(unblocked_edges_graph()).unwrap();
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

    let subscription = database.subscribe(unblocked_edges_graph()).unwrap();
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

    let subscription = database.subscribe(unblocked_edges_graph()).unwrap();
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

    let subscription = database.subscribe(unblocked_edges_graph()).unwrap();
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

    let subscription = database.subscribe(unblocked_edges_graph()).unwrap();
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
        .prepare(
            artist_album_shape_graph(),
            "artist_params",
            artist_binding_descriptor(),
            ["artist_id"],
        )
        .unwrap();
    let coltrane = database.bind_shape(shape.id(), &[Value::U64(7)]).unwrap();
    let miles = database.bind_shape(shape.id(), &[Value::U64(8)]).unwrap();

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
        .prepare(
            artist_album_shape_graph(),
            "artist_params",
            artist_binding_descriptor(),
            ["artist_id"],
        )
        .unwrap();
    let first = database.bind_shape(shape.id(), &[Value::U64(7)]).unwrap();
    let second = database.bind_shape(shape.id(), &[Value::U64(7)]).unwrap();

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
        vec!["id", "title", "artist"]
    );
    let sub = database
        .bind(&prepared, &[("artist", Value::U64(7))])
        .unwrap();

    assert_eq!(
        expect_try_recv_vals(&sub),
        vec![(
            vec![
                Value::U64(1),
                Value::String("Blue Train".to_owned()),
                Value::U64(7),
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
        expect_try_recv_vals(&sub),
        vec![(
            vec![
                Value::U64(3),
                Value::String("Giant Steps".to_owned()),
                Value::U64(7),
            ],
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
        .prepare(
            graph,
            "title_neq_params",
            binding_descriptor,
            ["title_param"],
        )
        .unwrap();
    let sub = database
        .bind_shape(prepared.id(), &[Value::String("Blue Train".to_owned())])
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
            .prepare(graph, "artist_params", binding_descriptor, ["missing"])
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
        .prepare(graph, "artist_params", binding_descriptor, ["artist_id"])
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
fn prepared_subscription_matches_literal_subscription_modulo_param_columns() {
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
        strip_artist_param(expect_try_recv_vals(&param_sub)),
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
        strip_artist_param(expect_try_recv_vals(&param_sub)),
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
            let [Value::U64(id), Value::String(title), Value::U64(_artist)] = values.as_slice()
            else {
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

fn strip_artist_param(rows: Vec<(Vec<Value>, i64)>) -> Vec<(Vec<Value>, i64)> {
    rows.into_iter()
        .map(|(values, weight)| {
            let [id, title, Value::U64(_artist)] = values.as_slice() else {
                panic!("unexpected prepared row shape: {values:?}");
            };
            (vec![id.clone(), title.clone()], weight)
        })
        .collect()
}

#[test]
fn binding_sources_are_rejected_outside_prepared_shapes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
    let mut database = Database::new(albums_artists_schema(), storage).unwrap();

    assert!(database.subscribe(artist_album_shape_graph()).is_err());
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
    let first = database.subscribe(graph.clone()).unwrap();
    let second = database.subscribe(graph).unwrap();

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
        database.subscribe(table_graph.clone()).unwrap();
        database.subscribe(index_graph.clone()).unwrap();

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
        let table_subscription_id = database.subscribe(table_graph).unwrap();
        let index_subscription_id = database.subscribe(index_graph).unwrap();

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
        database.subscribe(table_graph.clone()).unwrap();
        database.subscribe(index_graph.clone()).unwrap();

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
        let table_subscription_id = database.subscribe(table_graph).unwrap();
        let index_subscription_id = database.subscribe(index_graph).unwrap();

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
        .prepare(
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
                        .bind_shape(shape.id(), &[Value::U64(param)])
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
                let mut subscription =
                    OracleSubscription::new(graph, database.subscribe(builder).unwrap(), step);
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

fn track_values(id: u64, album: u64, disc: Option<u64>, title: &str) -> Vec<Value> {
    vec![
        Value::U64(id),
        Value::U64(album),
        Value::Nullable(disc.map(|value| Box::new(Value::U64(value)))),
        Value::String(title.to_owned()),
    ]
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
