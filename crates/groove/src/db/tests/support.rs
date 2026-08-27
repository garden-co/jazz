//! Shared schemas, graph builders, fixtures, and assertion helpers.

use super::*;

pub(super) fn version_zero_payload(stored: &[u8]) -> &[u8] {
    let (version, payload) = crate::records::split_variant_record(stored).unwrap();
    assert_eq!(version, 0);
    payload
}

pub(super) fn albums_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

pub(super) fn indexed_albums_schema() -> DatabaseSchema {
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

pub(super) fn unique_indexed_albums_schema() -> DatabaseSchema {
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

pub(super) fn scan_spec_schema() -> DatabaseSchema {
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

pub(super) fn insert_scan_doc(
    batch: &mut DatabaseBatch,
    tenant: &str,
    id: u64,
    path: &str,
    payload: &[u8],
) {
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

pub(super) fn uuid(value: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(value)
}

pub(super) fn indexed_tracks_schema() -> DatabaseSchema {
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

pub(super) fn track_values(id: u64, album_id: u64, disc: Option<u64>, title: &str) -> Vec<Value> {
    vec![
        Value::U64(id),
        Value::U64(album_id),
        Value::Nullable(disc.map(|disc| Box::new(Value::U64(disc)))),
        Value::String(title.to_owned()),
    ]
}

pub(super) fn history_schema() -> DatabaseSchema {
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

pub(super) fn two_history_tables_schema() -> DatabaseSchema {
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

pub(super) fn history_values(row: u64, stamp: u64, node: u64, title: &str) -> Vec<Value> {
    vec![
        Value::U64(row),
        Value::U64(stamp),
        Value::U64(node),
        Value::String(title.to_owned()),
    ]
}

pub(super) fn history_key(row: u64, stamp: u64, node: u64) -> PrimaryKeyValue {
    PrimaryKeyValue::Composite(vec![
        PrimaryKeyValue::U64(row),
        PrimaryKeyValue::U64(stamp),
        PrimaryKeyValue::U64(node),
    ])
}

pub(super) fn collect_tree_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "tree",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("root", ColumnType::U64),
            ColumnSchema::new("child", ColumnType::U64),
            ColumnSchema::new("child_order", ColumnType::U64),
            ColumnSchema::new("grandchild", ColumnType::U64),
            ColumnSchema::new("grandchild_order", ColumnType::U64),
            ColumnSchema::new("left", ColumnType::U64),
            ColumnSchema::new("left_order", ColumnType::U64),
            ColumnSchema::new("right", ColumnType::U64),
            ColumnSchema::new("right_order", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

pub(super) fn routed_collect_tree_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "routed_tree",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("root", ColumnType::U64),
            ColumnSchema::new("route", ColumnType::U64),
            ColumnSchema::new("child", ColumnType::U64),
            ColumnSchema::new("child_order", ColumnType::U64),
            ColumnSchema::new("grandchild", ColumnType::U64),
            ColumnSchema::new("grandchild_order", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

pub(super) fn collect_tree_values(
    [
        id,
        child,
        child_order,
        grandchild,
        grandchild_order,
        left,
        left_order,
        right,
        right_order,
    ]: [u64; 9],
) -> Vec<Value> {
    vec![
        Value::U64(id),
        Value::U64(1),
        Value::U64(child),
        Value::U64(child_order),
        Value::U64(grandchild),
        Value::U64(grandchild_order),
        Value::U64(left),
        Value::U64(left_order),
        Value::U64(right),
        Value::U64(right_order),
    ]
}

pub(super) fn collect_tree_graph() -> GraphBuilder {
    GraphBuilder::collect_by_tree(
        GraphBuilder::table("tree"),
        ["root"],
        [CollectByField::named("root")],
        [
            CollectBySlotBuilder::new(
                ["root"],
                [CollectByField::named("child")],
                "children",
                [CollectBySlotBuilder::new(
                    ["child"],
                    [CollectByField::named("grandchild")],
                    "grandchildren",
                    [],
                    [TopByOrder::asc("grandchild_order")],
                    ["grandchild"],
                    0,
                    TopByLimit::Finite(2),
                )],
                [TopByOrder::asc("child_order")],
                ["child"],
                0,
                TopByLimit::Finite(2),
            ),
            CollectBySlotBuilder::new(
                ["root"],
                [CollectByField::named("left")],
                "lefts",
                [],
                [TopByOrder::asc("left_order")],
                ["left"],
                0,
                TopByLimit::Finite(2),
            ),
            CollectBySlotBuilder::new(
                ["root"],
                [CollectByField::named("right")],
                "rights",
                [],
                [TopByOrder::desc("right_order")],
                ["right"],
                1,
                TopByLimit::Finite(1),
            ),
        ],
    )
}

pub(super) fn routed_collect_tree_graph() -> GraphBuilder {
    GraphBuilder::collect_by_tree(
        GraphBuilder::table("routed_tree"),
        ["root", "route"],
        [CollectByField::named("root")],
        [CollectBySlotBuilder::new(
            ["root", "route"],
            [CollectByField::named("child")],
            "children",
            [CollectBySlotBuilder::new(
                ["child", "route"],
                [CollectByField::named("grandchild")],
                "grandchildren",
                [],
                [TopByOrder::asc("grandchild_order")],
                ["grandchild"],
                0,
                TopByLimit::Unbounded,
            )],
            [TopByOrder::asc("child_order")],
            ["child"],
            0,
            TopByLimit::Unbounded,
        )
        .with_owner_key_cols(["route"])],
    )
}

pub(super) fn history_arg_max() -> GraphBuilder {
    GraphBuilder::arg_max_by(GraphBuilder::table("history"), ["row"], ["stamp", "node"])
}

pub(super) fn history_arg_min() -> GraphBuilder {
    GraphBuilder::arg_min_by(GraphBuilder::table("history"), ["row"], ["stamp", "node"])
}

pub(super) fn history_top_by_stamp_asc(limit: u64) -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("history"),
        ["row"],
        [TopByOrder::asc("stamp")],
        ["node"],
        0,
        TopByLimit::Finite(limit),
    )
}

pub(super) fn history_top_by_stamp_asc_unbounded() -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("history"),
        ["row"],
        [TopByOrder::asc("stamp")],
        ["node"],
        0,
        TopByLimit::Unbounded,
    )
}

pub(super) fn history_top_by_stamp_desc(limit: u64) -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("history"),
        ["row"],
        [TopByOrder::desc("stamp")],
        ["node"],
        0,
        TopByLimit::Finite(limit),
    )
}

pub(super) fn history_top_by_stamp_asc_offset(offset: u64, limit: u64) -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("history"),
        ["row"],
        [TopByOrder::asc("stamp")],
        ["node"],
        offset,
        TopByLimit::Finite(limit),
    )
}

pub(super) fn history_collect_by(limit: u64) -> GraphBuilder {
    GraphBuilder::collect_by(
        GraphBuilder::table("history"),
        ["row"],
        [CollectByField::named("row")],
        [
            CollectByField::renamed("node", "child_id"),
            CollectByField::renamed("title", "child_title"),
        ],
        "children",
        [TopByOrder::asc("stamp")],
        ["node"],
        0,
        TopByLimit::Finite(limit),
    )
}

pub(super) fn history_collect_by_expand(offset: u64, limit: u64) -> GraphBuilder {
    GraphBuilder::collect_by_expand(
        GraphBuilder::table("history"),
        ["row"],
        [
            CollectByField::named("row"),
            CollectByField::renamed("node", "source_node"),
            CollectByField::renamed("title", "child_title"),
        ],
        ["row", "node"],
        [TopByOrder::asc("stamp")],
        ["node"],
        offset,
        TopByLimit::Finite(limit),
    )
}

pub(super) fn collect_parent(row: u64, children: &[(u64, &str)]) -> Vec<Value> {
    let child_descriptor = RecordDescriptor::new([
        ("child_id", ValueType::U64),
        ("child_title", ValueType::String),
    ]);
    vec![
        Value::U64(row),
        Value::Array(
            children
                .iter()
                .map(|(id, title)| {
                    Value::Record(crate::records::OwnedRecord::new(
                        child_descriptor
                            .create(&[Value::U64(*id), Value::String((*title).to_owned())])
                            .unwrap(),
                        child_descriptor,
                    ))
                })
                .collect(),
        ),
    ]
}

pub(super) fn reachability_collect_by(limit: u64) -> GraphBuilder {
    GraphBuilder::collect_by(
        reachability_graph(32),
        ["src"],
        [CollectByField::named("src")],
        [CollectByField::renamed("dst", "child_id")],
        "children",
        [TopByOrder::asc("dst")],
        ["dst"],
        0,
        TopByLimit::Finite(limit),
    )
}

pub(super) fn nullable_scores_schema() -> DatabaseSchema {
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

pub(super) fn uuid_docs_schema() -> DatabaseSchema {
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

pub(super) fn nullable_routed_docs_schema() -> DatabaseSchema {
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

pub(super) fn enum_tasks_schema() -> DatabaseSchema {
    let status = ColumnType::EnumTag(
        ScalarEnumSchema::new("task_status", ["todo", "doing", "done"]).unwrap(),
    );
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

pub(super) fn payload_enum_type(
    registry_id: u64,
    cases: impl IntoIterator<Item = (&'static str, ValueType)>,
) -> ValueType {
    ValueType::Enum(Box::new(
        EnumSchema::new(
            "state",
            cases.into_iter().map(|(name, value_type)| {
                EnumCase::new(name, RecordDescriptor::new([("value", value_type)]))
            }),
        )
        .unwrap()
        .with_registry_id(registry_id),
    ))
}

pub(super) fn live_variant_enum_table(state: ValueType) -> TableSchema {
    TableSchema::new_with_bound_registries(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("state", state.clone()),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_variant_payload(
        1,
        [
            TableVariantField::shared("id", ColumnType::U64, "id"),
            TableVariantField::shared("state", state, "state"),
        ],
    )
}

/// A live variant table advances one direct payload-enum registry, then adds a
/// new row layout. Existing layouts must use the widened descriptor too, so
/// old and new rows share one durable physical registry after restart.
pub(super) fn tuple_edges_schema() -> DatabaseSchema {
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

pub(super) fn interval_history_schema() -> DatabaseSchema {
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

pub(super) fn nullable_markers_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "markers",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("marker", ColumnType::String.nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

pub(super) fn nested_nullable_markers_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "markers",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("marker", ColumnType::String.nullable().nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

pub(super) fn two_album_tables_schema() -> DatabaseSchema {
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

pub(super) fn albums_artists_schema() -> DatabaseSchema {
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

pub(super) fn files_parts_schema() -> DatabaseSchema {
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

pub(super) fn indexed_files_schema() -> DatabaseSchema {
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

pub(super) fn nullable_files_parts_schema() -> DatabaseSchema {
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

pub(super) fn albums_blockers_schema() -> DatabaseSchema {
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

pub(super) fn tenant_albums_artists_schema() -> DatabaseSchema {
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

pub(super) fn edges_schema() -> DatabaseSchema {
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

pub(super) fn edges_docs_schema() -> DatabaseSchema {
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

pub(super) fn edges_blockers_schema() -> DatabaseSchema {
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

pub(super) fn integer_key_widths_schema() -> DatabaseSchema {
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

pub(super) fn composite_key_schema() -> DatabaseSchema {
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

pub(super) fn expect_recv_vals(subscription: &Subscription) -> Vec<(Vec<Value>, i64)> {
    loop {
        let deltas = subscription.recv().unwrap();
        if !deltas.is_empty() {
            let mut values = deltas.to_values().unwrap();
            values.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            return values;
        }
    }
}

pub(super) fn expect_try_recv_vals(subscription: &Subscription) -> Vec<(Vec<Value>, i64)> {
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

pub(super) fn col(name: &str) -> Expr {
    Expr::column(name)
}

pub(super) fn qcol(qualifier: &str, name: &str) -> Expr {
    Expr::Column(ColumnRef::qualified([qualifier], name))
}

pub(super) fn select_query(select: Select) -> Query {
    Query::Select(Box::new(select))
}

pub(super) fn reachability_graph(max_iters: usize) -> GraphBuilder {
    let reach = RecordDescriptor::new([
        ("src", ColumnType::U64.clone()),
        ("dst", ColumnType::U64.clone()),
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

pub(super) fn prepared_reachability_graph(
    edge_input: GraphBuilder,
    max_iters: usize,
) -> GraphBuilder {
    let reach = RecordDescriptor::new([
        ("seed", ColumnType::U64.clone()),
        ("dst", ColumnType::U64.clone()),
    ]);
    let seed = GraphBuilder::binding_source(
        "prepared-reach",
        RecordDescriptor::new([("seed", ColumnType::U64.clone())]),
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

pub(super) async fn prepared_reachability_shape(
    database: &mut Database,
) -> crate::ivm::PreparedShape {
    database
        .prepare_one_sink(
            prepared_reachability_graph(GraphBuilder::table("edges"), 16),
            "prepared-reach",
            RecordDescriptor::new([("seed", ColumnType::U64.clone())]),
            ["seed".to_owned()],
        )
        .await
        .unwrap()
}

pub(super) async fn prepared_reachability_with_antijoin_shape(
    database: &mut Database,
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
            RecordDescriptor::new([("seed", ColumnType::U64.clone())]),
            ["seed".to_owned()],
        )
        .await
        .unwrap()
}

pub(super) fn two_hop_graph() -> GraphBuilder {
    let left = GraphBuilder::table("edges").project(["src", "dst"]);
    let right = GraphBuilder::table("edges").project(["src", "dst"]);
    GraphBuilder::join(left, right, ["dst"], ["src"]).project_fields([
        ProjectField::renamed("left.src", "src"),
        ProjectField::renamed("right.dst", "dst"),
    ])
}

pub(super) fn unblocked_edges_graph() -> GraphBuilder {
    GraphBuilder::anti_join(
        GraphBuilder::table("edges"),
        GraphBuilder::table("blockers"),
        ["src", "dst"],
        ["src", "dst"],
    )
    .project(["src", "dst"])
}

pub(super) fn artist_album_shape_graph() -> GraphBuilder {
    let params = GraphBuilder::binding_source(
        "artist_params",
        RecordDescriptor::new([("artist_id", ColumnType::U64.clone())]),
    );
    let albums = GraphBuilder::table("albums").project(["artist_id", "id", "title"]);
    GraphBuilder::join(params, albums, ["artist_id"], ["artist_id"]).project_fields([
        ProjectField::renamed("left.artist_id", "artist_id"),
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
    ])
}

pub(super) fn artist_binding_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("artist_id", ColumnType::U64.clone())])
}

pub(super) fn insert_edge(batch: &mut DatabaseBatch, id: u64, src: u64, dst: u64) {
    batch.insert(
        "edges",
        vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
    );
}

pub(super) fn grant_shape_schema() -> DatabaseSchema {
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

pub(super) fn grant_shape_graph() -> GraphBuilder {
    let binding_descriptor = RecordDescriptor::new([("seed", ColumnType::U64.clone())]);
    let reach_descriptor = RecordDescriptor::new([
        ("seed", ColumnType::U64.clone()),
        ("group", ColumnType::U64.clone()),
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

pub(super) async fn prepare_grant_shape(database: &mut Database) -> crate::ivm::PreparedShape {
    database
        .prepare_one_sink(
            grant_shape_graph(),
            "grant-claim",
            RecordDescriptor::new([("seed", ColumnType::U64.clone())]),
            ["seed"],
        )
        .await
        .unwrap()
}

pub(super) fn insert_group_edge(batch: &mut DatabaseBatch, id: u64, src: u64, dst: u64) {
    batch.insert(
        "group_edges",
        vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
    );
}

pub(super) fn insert_access_edge(batch: &mut DatabaseBatch, id: u64, resource: u64, group: u64) {
    batch.insert(
        "access_edges",
        vec![Value::U64(id), Value::U64(resource), Value::U64(group)],
    );
}

pub(super) fn insert_resource(batch: &mut DatabaseBatch, id: u64, payload: u64) {
    batch.insert("resources", vec![Value::U64(id), Value::U64(payload)]);
}

pub(super) fn update_edge(batch: &mut DatabaseBatch, id: u64, src: u64, dst: u64) {
    batch.update(
        "edges",
        vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
    );
}

pub(super) fn sort_pairs_by_value(values: &mut [(Vec<Value>, i64)]) {
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

pub(super) fn prepared_reachability_oracle(
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

pub(super) fn seeded_positive_edge_insertions() -> Vec<(u64, u64)> {
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

// Shared durable-index encoding assertions.

pub(super) fn record_values(records: Vec<VariantRecord>) -> Vec<Vec<Value>> {
    records
        .into_iter()
        .map(|record| record.to_values().unwrap())
        .collect()
}
pub(super) fn encoded_title_index_key(title: &str, primary_key: u64) -> Vec<u8> {
    let mut bytes = encoded_title_key_part(title);
    bytes.push(0xff);
    bytes.extend(encoded_u64_index_part(primary_key));
    bytes
}

pub(super) fn encoded_uuid_index_key(value: uuid::Uuid, primary_key: u64) -> Vec<u8> {
    let mut bytes = vec![10];
    bytes.extend(value.as_bytes());
    bytes.push(0xff);
    bytes.extend(encoded_u64_index_part(primary_key));
    bytes
}

pub(super) fn encoded_history_by_tx_key(stamp: u64, node: u64, row: u64) -> Vec<u8> {
    let mut bytes = encoded_u64_index_part(stamp);
    bytes.extend(encoded_u64_index_part(node));
    bytes.extend(encoded_u64_index_part(row));
    bytes
}

pub(super) fn encoded_title_key_part(title: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(6);
    encode_ordered_bytes(&mut bytes, title.as_bytes());
    bytes
}

pub(super) fn persisted_index_storage_key(index: &str, logical_key: &[u8]) -> Vec<u8> {
    persisted_table_index_storage_key("albums", index, logical_key)
}

pub(super) fn persisted_table_index_storage_key(
    table: &str,
    index: &str,
    logical_key: &[u8],
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(table.as_bytes());
    bytes.push(0);
    bytes.extend(index.as_bytes());
    bytes.push(0);
    bytes.extend(encoded_bytes_key_part(logical_key));
    bytes
}

pub(super) fn encoded_bytes_key_part(value: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(7);
    encode_ordered_bytes(&mut bytes, value);
    bytes
}

pub(super) fn encoded_u64_index_part(value: u64) -> Vec<u8> {
    let mut bytes = vec![3];
    bytes.extend(value.to_be_bytes());
    bytes
}

pub(super) fn encode_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.extend([0, 0]);
}

pub(super) fn persisted_index_value(record: &[u8]) -> Vec<u8> {
    let descriptor = RecordDescriptor::new([
        ("key", crate::records::ValueType::Bytes),
        ("value", crate::records::ValueType::Bytes),
    ]);
    match descriptor.get(record, "value").unwrap() {
        Value::Bytes(value) => value,
        value => panic!("expected persisted index value bytes, got {value:?}"),
    }
}

// Shared payload-enum schema and values.

pub(super) fn open_task_payload_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("priority", ValueType::U64), ("title", ValueType::String)])
}

pub(super) fn closed_task_payload_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("reason", ValueType::String)])
}

pub(super) fn payload_enum_tasks_schema() -> DatabaseSchema {
    let task_state = ColumnType::Enum(Box::new(
        EnumSchema::new(
            "task_state",
            [
                EnumCase::new("open", open_task_payload_descriptor()),
                EnumCase::new("closed", closed_task_payload_descriptor()),
            ],
        )
        .unwrap(),
    ))
    .nullable();
    DatabaseSchema::new([TableSchema::new(
        "payload_tasks",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("state", task_state),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

pub(super) fn open_task(priority: u64, title: &str) -> Value {
    Value::Enum(
        EnumValue::create(
            0,
            open_task_payload_descriptor(),
            &[Value::U64(priority), Value::String(title.to_owned())],
        )
        .unwrap(),
    )
}

pub(super) fn closed_task(reason: &str) -> Value {
    Value::Enum(
        EnumValue::create(
            1,
            closed_task_payload_descriptor(),
            &[Value::String(reason.to_owned())],
        )
        .unwrap(),
    )
}
