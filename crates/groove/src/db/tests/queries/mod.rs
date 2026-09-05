//! One-shot query lowering, nullable operators, windows, collectors, and aggregates.

use super::*;

#[futures_test::test]
async fn query_returns_filtered_current_rows() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Too Early".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(11), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();
    assert_eq!(
        result.to_values().unwrap(),
        [(vec!["Blue Train".into()], 1)]
    );
}

#[futures_test::test]
async fn enum_predicates_resolve_variant_names_at_plan_time() {
    let storage = MemoryStorage::new(&["tasks", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(enum_tasks_schema(), storage).await.unwrap();

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
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();
    assert_eq!(result.to_values().unwrap(), [(vec!["two".into()], 1)]);
}

#[futures_test::test]
async fn enum_index_keys_follow_declaration_order() {
    let storage = MemoryStorage::new(&["tasks", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(enum_tasks_schema(), storage).await.unwrap();

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
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        record_values(
            database
                .index_scan("tasks", "tasks_by_status", &[])
                .await
                .unwrap()
        )
        .into_iter()
        .map(|values| values[1].clone())
        .collect::<Vec<_>>(),
        vec![Value::EnumTag(0), Value::EnumTag(1), Value::EnumTag(2)]
    );
    assert_eq!(
        record_values(
            database
                .index_get(
                    "tasks",
                    "tasks_by_status",
                    &[Value::String("doing".to_owned())]
                )
                .await
                .unwrap()
        )
        .into_iter()
        .map(|values| values[3].clone())
        .collect::<Vec<_>>(),
        vec![Value::String("task-3".to_owned())]
    );
}

#[futures_test::test]
async fn nullable_comparisons_unwrap_present_values_and_skip_nulls() {
    let storage = MemoryStorage::new(&["markers"]).expect("valid memory storage families");
    let mut database = Database::new(nullable_markers_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("markers", vec![Value::U64(1), Value::Nullable(None)]);
    batch.insert(
        "markers",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::String("deleted".to_owned())))),
        ],
    );
    database.commit_batch(batch).await.unwrap();

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
        .await
        .unwrap();
    assert_eq!(result.to_values().unwrap(), [(vec![Value::U64(2)], 1)]);
}

#[futures_test::test]
async fn query_lowers_is_null_and_is_not_null_predicates() {
    let storage = MemoryStorage::new(&["markers"]).expect("valid memory storage families");
    let mut database = Database::new(nullable_markers_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("markers", vec![Value::U64(1), Value::Nullable(None)]);
    batch.insert(
        "markers",
        vec![
            Value::U64(2),
            Value::Nullable(Some(Box::new(Value::String("present".to_owned())))),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let is_null = database
        .query(select_query(
            Select::new([SelectItem::expr(col("id"))])
                .from([TableRef::named("markers")])
                .where_(Expr::Unary {
                    op: UnaryOp::IsNull,
                    expr: Box::new(col("marker")),
                }),
        ))
        .await
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
        .await
        .unwrap();

    assert_eq!(is_null.to_values().unwrap(), [(vec![Value::U64(1)], 1)]);
    assert_eq!(is_not_null.to_values().unwrap(), [(vec![Value::U64(2)], 1)]);
}

#[futures_test::test]
async fn is_null_matches_nested_nullable_none() {
    let storage = MemoryStorage::new(&["markers"]).expect("valid memory storage families");
    let mut database = Database::new(nested_nullable_markers_schema(), storage)
        .await
        .unwrap();

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
    database.commit_batch(batch).await.unwrap();

    let is_null = database
        .query(select_query(
            Select::new([SelectItem::expr(col("id"))])
                .from([TableRef::named("markers")])
                .where_(Expr::Unary {
                    op: UnaryOp::IsNull,
                    expr: Box::new(col("marker")),
                }),
        ))
        .await
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
        .await
        .unwrap();

    assert_eq!(is_null.to_values().unwrap(), [(vec![Value::U64(1)], 1)]);
    assert_eq!(is_not_null.to_values().unwrap(), [(vec![Value::U64(2)], 1)]);
}

#[futures_test::test]
async fn unwrap_nullable_graph_drops_none_and_unwraps_present_values() {
    let storage =
        MemoryStorage::new(&["tracks", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_tracks_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    batch.insert("tracks", track_values(2, 7, None, "Hidden"));
    batch.insert("tracks", track_values(3, 7, Some(2), "Outro"));
    database.commit_batch(batch).await.unwrap();

    let result = database
        .query_graph(
            GraphBuilder::table("tracks")
                .unwrap_nullable("disc")
                .project(["id", "disc"]),
        )
        .await
        .unwrap();
    assert_eq!(
        result.to_values().unwrap(),
        [
            (vec![Value::U64(1), Value::U64(1)], 1),
            (vec![Value::U64(3), Value::U64(2)], 1),
        ]
    );
}

#[futures_test::test]
#[ignore = "#1787: receipt-only timing for batch-general schema index maintenance"]
async fn indexed_batch_commit_timing_receipt_20k_and_single_row() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    const ROWS: u64 = 20_000;
    let storage =
        MemoryStorage::new(&["tracks", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_tracks_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    for id in 0..ROWS {
        batch.insert(
            "tracks",
            track_values(id, id % 30, Some(id % 5), &format!("bulk-track-{id:05}")),
        );
    }

    let bulk_start = Instant::now();
    database.commit_batch(batch).await.unwrap();
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
        .await
        .unwrap();
    assert_eq!(album_rows.len(), 667);
    assert_eq!(
        database
            .index_get(
                "tracks",
                "tracks_by_title_unique",
                &[Value::String("bulk-track-12345".to_owned())],
            )
            .await
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
    database.commit_batch(single).await.unwrap();
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
            .await
            .unwrap()
            .len()
    );
}

#[futures_test::test]
async fn query_graphs_returns_named_one_shot_snapshots() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(2), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let snapshots = database
        .query_graphs([
            ("ids", GraphBuilder::table("albums").project(["id"])),
            ("titles", GraphBuilder::table("albums").project(["title"])),
        ])
        .await
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

#[futures_test::test]
async fn unwrap_nullable_retractions_flow_symmetrically() {
    let storage =
        MemoryStorage::new(&["tracks", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_tracks_schema(), storage)
        .await
        .unwrap();
    let subscription = database
        .subscribe_one_sink(
            GraphBuilder::table("tracks")
                .unwrap_nullable("disc")
                .project(["id", "disc"]),
        )
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(1)], 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("tracks", PrimaryKeyValue::U64(1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(1)], -1)]
    );
}

#[futures_test::test]
async fn project_nullable_wraps_uuid_and_string_fields() {
    let storage = MemoryStorage::new(&["docs", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(uuid_docs_schema(), storage).await.unwrap();
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
    database.commit_batch(batch).await.unwrap();

    let result = database
        .query_graph(GraphBuilder::table("docs").project_fields([
            ProjectField::nullable("id", "maybe_id"),
            ProjectField::nullable("title", "maybe_title"),
        ]))
        .await
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

#[futures_test::test]
async fn project_nullable_can_union_with_typed_null_projection() {
    let storage = MemoryStorage::new(&["docs", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(uuid_docs_schema(), storage).await.unwrap();
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
    database.commit_batch(batch).await.unwrap();

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
        .await
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

#[futures_test::test]
async fn query_returns_empty_result_for_empty_answers() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

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
        .await
        .unwrap();

    assert!(result.is_empty());
}

#[futures_test::test]
async fn table_static_scan_specs_hydrate_like_full_scan_then_filter() {
    let storage = MemoryStorage::new(&["docs", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(scan_spec_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    insert_scan_doc(&mut batch, "a", 1, "/alpha", b"\0first");
    insert_scan_doc(&mut batch, "a", 2, "/beta", b"second");
    insert_scan_doc(&mut batch, "équipe", 1, "/unicode", b"\xffthird");
    insert_scan_doc(&mut batch, "z", 1, "/zeta", b"last");
    database.commit_batch(batch).await.unwrap();

    let prefix = database
        .query_graph(GraphBuilder::table_scan(
            "docs",
            StaticScanSpec::Prefix(vec![LiteralValue::String("a".to_owned())]),
        ))
        .await
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
        .await
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
        .await
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
        .await
        .unwrap();
    assert!(empty.is_empty());
}

#[futures_test::test]
async fn index_static_scan_specs_filter_index_records() {
    let storage = MemoryStorage::new(&["docs", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(scan_spec_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    insert_scan_doc(&mut batch, "a", 1, "/alpha", b"first");
    insert_scan_doc(&mut batch, "b", 2, "/alpha", b"second");
    insert_scan_doc(&mut batch, "b", 3, "/beta", b"third");
    database.commit_batch(batch).await.unwrap();

    let prefix = database
        .query_graph(GraphBuilder::index_scan(
            "docs",
            "docs_by_path",
            StaticScanSpec::Prefix(vec![LiteralValue::String("/alpha".to_owned())]),
        ))
        .await
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
        .await
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
        .await
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(range.len(), 2);
}

#[futures_test::test]
async fn static_scan_specs_participate_in_node_identity() {
    let storage = MemoryStorage::new(&["docs", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(scan_spec_schema(), storage).await.unwrap();

    let same_a = GraphBuilder::table_scan(
        "docs",
        StaticScanSpec::Prefix(vec![LiteralValue::String("a".to_owned())]),
    );
    let same_b = same_a.clone();
    let different = GraphBuilder::table_scan(
        "docs",
        StaticScanSpec::Prefix(vec![LiteralValue::String("b".to_owned())]),
    );

    let first_subscription = database.subscribe_one_sink(same_a).await.unwrap();
    let after_first = database.ivm_runtime.graph().nodes().len();
    let second_subscription = database.subscribe_one_sink(same_b).await.unwrap();
    let after_same = database.ivm_runtime.graph().nodes().len();
    let different_subscription = database.subscribe_one_sink(different).await.unwrap();
    let after_different = database.ivm_runtime.graph().nodes().len();

    assert_eq!(after_first, after_same);
    assert!(after_different > after_same);
    drop((
        first_subscription,
        second_subscription,
        different_subscription,
    ));
}

#[futures_test::test]
async fn one_shot_static_scan_does_not_perturb_existing_subscription() {
    let storage = MemoryStorage::new(&["docs", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(scan_spec_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("docs").project(["tenant", "id"]))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    insert_scan_doc(&mut batch, "a", 1, "/alpha", b"first");
    insert_scan_doc(&mut batch, "b", 2, "/beta", b"second");
    database.commit_batch(batch).await.unwrap();
    let initial = expect_recv_vals(&subscription);
    assert_eq!(initial.len(), 2);

    let queried = database
        .query_graph(GraphBuilder::table_scan(
            "docs",
            StaticScanSpec::Prefix(vec![LiteralValue::String("a".to_owned())]),
        ))
        .await
        .unwrap();
    assert_eq!(queried.deltas.len(), 1);

    let mut batch = database.open_batch();
    insert_scan_doc(&mut batch, "c", 3, "/gamma", b"third");
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        expect_recv_vals(&subscription),
        [(vec![Value::String("c".to_owned()), Value::U64(3)], 1)]
    );
}

#[futures_test::test]
async fn subscribe_supports_recursive_hydration_snapshot_message() {
    let storage = MemoryStorage::new(&["edges"]).expect("valid memory storage families");
    let mut database = Database::new(edges_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    insert_edge(&mut batch, 1, 1, 2);
    insert_edge(&mut batch, 2, 2, 3);
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(reachability_graph(16))
        .await
        .unwrap();
    database.flush().await.unwrap();
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
    database.commit_batch(batch).await.unwrap();
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

mod aggregates;
mod composition;
mod structured;
mod windows;

#[futures_test::test]
async fn stored_name_projection_keeps_exact_carrier_with_duplicate_logical_names() {
    use crate::ivm::{FieldRef, ProjectExpr};
    use crate::records::{DescriptorField, FieldIdentity};
    let descriptor = RecordDescriptor::new_with_fields([
        DescriptorField::new("left_carrier", ValueType::U64).with_identity(
            FieldIdentity::NamedSlot {
                name: "shared".into(),
                slot: 1,
            },
        ),
        DescriptorField::new("right_carrier", ValueType::U64).with_identity(
            FieldIdentity::NamedSlot {
                name: "shared".into(),
                slot: 2,
            },
        ),
    ]);
    let storage = MemoryStorage::new(&[]).unwrap();
    let mut database = Database::new(DatabaseSchema::new([]), storage)
        .await
        .unwrap();
    for field in [
        FieldRef::name("right_carrier"),
        FieldRef::stored_name("right_carrier"),
    ] {
        let graph = GraphBuilder::values(descriptor, [vec![Value::U64(11), Value::U64(22)]])
            .unwrap()
            .project_fields([ProjectField {
                expression: ProjectExpr::Field(field.clone()),
                output_name: "selected".into(),
                output_identity: FieldIdentity::Name("selected".into()),
            }]);
        let actual = database
            .query_graph(graph)
            .await
            .unwrap()
            .to_values()
            .unwrap();
        assert_eq!(actual, [(vec![Value::U64(22)], 1)], "reference {field:?}");
    }
}

#[futures_test::test]
async fn stored_name_projection_uses_unambiguous_logical_name_when_carrier_is_shadowed() {
    use crate::ivm::{FieldRef, ProjectExpr};
    use crate::records::{DescriptorField, FieldIdentity};
    let descriptor = RecordDescriptor::new_with_fields([
        DescriptorField::new("user_title", ValueType::U64)
            .with_identity(FieldIdentity::Name("title".into())),
        DescriptorField::new("user_user_title", ValueType::U64)
            .with_identity(FieldIdentity::Name("user_title".into())),
    ]);
    let mut database = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap();
    for (carrier, expected) in [("user_title", 11), ("user_user_title", 22)] {
        let graph = GraphBuilder::values(descriptor, [vec![Value::U64(11), Value::U64(22)]])
            .unwrap()
            .project_fields([ProjectField {
                expression: ProjectExpr::Field(FieldRef::stored_name(carrier)),
                output_name: "selected".into(),
                output_identity: FieldIdentity::Name("selected".into()),
            }]);
        assert_eq!(
            database
                .query_graph(graph)
                .await
                .unwrap()
                .to_values()
                .unwrap(),
            [(vec![Value::U64(expected)], 1)]
        );
    }
}

#[futures_test::test]
async fn stored_name_projection_preserves_exact_selection_under_irreducible_name_ambiguity() {
    use crate::ivm::{FieldRef, ProjectExpr};
    use crate::records::{DescriptorField, FieldIdentity};
    let descriptor = RecordDescriptor::new_with_fields([
        DescriptorField::new("first", ValueType::U64)
            .with_identity(FieldIdentity::Name("right_carrier".into())),
        DescriptorField::new("second", ValueType::U64).with_identity(FieldIdentity::NamedSlot {
            name: "shared".into(),
            slot: 1,
        }),
        DescriptorField::new("right_carrier", ValueType::U64).with_identity(
            FieldIdentity::NamedSlot {
                name: "shared".into(),
                slot: 2,
            },
        ),
    ]);
    let mut database = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap();
    for (expression, expected) in [
        (
            ProjectExpr::Field(FieldRef::stored_name("right_carrier")),
            Value::U64(33),
        ),
        (
            ProjectExpr::Nullable(FieldRef::stored_name("right_carrier")),
            Value::Nullable(Some(Box::new(Value::U64(33)))),
        ),
        (
            ProjectExpr::NullableFlat(FieldRef::stored_name("right_carrier")),
            Value::Nullable(Some(Box::new(Value::U64(33)))),
        ),
    ] {
        let graph = GraphBuilder::values(
            descriptor,
            [vec![Value::U64(11), Value::U64(22), Value::U64(33)]],
        )
        .unwrap()
        .project_fields([ProjectField {
            expression,
            output_name: "selected".into(),
            output_identity: FieldIdentity::Name("selected".into()),
        }]);
        assert_eq!(
            database
                .query_graph(graph)
                .await
                .unwrap()
                .to_values()
                .unwrap(),
            [(vec![expected], 1)]
        );
    }
}

#[futures_test::test]
async fn joined_output_carriers_do_not_rebind_to_colliding_application_names() {
    use crate::records::{DescriptorField, FieldIdentity};
    let descriptor = RecordDescriptor::new_with_fields([
        DescriptorField::new("_app_created_by", ValueType::U64)
            .with_identity(FieldIdentity::Name("created_by".into())),
        DescriptorField::new("created_by", ValueType::String),
    ]);
    let mut database = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap();
    let left = GraphBuilder::values(
        descriptor,
        [vec![Value::U64(33), Value::String("metadata".into())]],
    )
    .unwrap();
    let right = GraphBuilder::values(
        RecordDescriptor::new([("seed", ValueType::U8)]),
        [vec![Value::U8(7)]],
    )
    .unwrap();
    let graph = GraphBuilder::join(
        left,
        right,
        std::iter::empty::<String>(),
        std::iter::empty::<String>(),
    );
    assert_eq!(
        database
            .query_graph(graph)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(
            vec![
                Value::U64(33),
                Value::String("metadata".into()),
                Value::U8(7)
            ],
            1
        )]
    );
}
