//! Public bag, null-filtering and error contracts for consumer column pruning.

use groove::db::{Database, GraphBuilder, MultisinkDeltas, PrimaryKeyValue, SubscriptionLifetime};
use groove::ivm::ProjectField;
use groove::records::{RecordDescriptor, ScalarEnumSchema, Value, ValueType};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

fn bag(rows: Vec<(Vec<Value>, i64)>) -> Vec<(Vec<Value>, i64)> {
    let mut out: Vec<(Vec<Value>, i64)> = Vec::new();
    for (row, weight) in rows {
        if let Some((_, count)) = out.iter_mut().find(|(other, _)| *other == row) {
            *count += weight;
        } else {
            out.push((row, weight));
        }
    }
    out.retain(|(_, weight)| *weight != 0);
    out.sort_by_key(|row| format!("{:?}", row.0));
    out
}

// Incremental delivery omits sinks with no changes.
fn changes(delta: &MultisinkDeltas, sink: &str) -> Vec<(Vec<Value>, i64)> {
    bag(delta
        .get(sink)
        .map(|rows| rows.to_values().unwrap())
        .unwrap_or_default())
}

async fn first(db: &mut Database, graph: GraphBuilder) -> Vec<(Vec<Value>, i64)> {
    let sub = db
        .subscribe_with_lifetime([("rows", graph)], SubscriptionLifetime::FirstResult, None)
        .unwrap();
    let result = db.next_multisink_subscription(&sub).await.unwrap();
    bag(result.get("rows").unwrap().to_values().unwrap())
}

fn some(value: u64) -> Value {
    Value::Nullable(Some(Box::new(Value::U64(value))))
}

fn item(id: u64, key: Value, body: &str) -> Vec<Value> {
    vec![Value::U64(id), key, Value::String(body.into())]
}

#[futures_test::test]
async fn narrow_existence_paths_preserve_bags_and_shared_payloads_across_updates() {
    for anti in [false, true] {
        let items = TableSchema::new(
            "items",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("key", ColumnType::Nullable(Box::new(ColumnType::U64))),
                ColumnSchema::new("body", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64));
        let matches = TableSchema::new(
            "matches",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("key", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64));
        let mut db = Database::new(
            DatabaseSchema::new([items, matches]),
            MemoryStorage::new(&["items", "matches"]).unwrap(),
        )
        .await
        .unwrap();
        let mut batch = db.open_batch();
        batch.insert("items", item(1, some(7), "first payload"));
        batch.insert("items", item(2, some(7), "second payload"));
        batch.insert("items", item(3, Value::Nullable(None), "null payload"));
        batch.insert("matches", vec![Value::U64(10), Value::U64(7)]);
        batch.insert("matches", vec![Value::U64(11), Value::U64(7)]);
        db.commit_batch(batch).await.unwrap();

        let shared = GraphBuilder::table("items").project_fields([
            ProjectField::renamed("body", "payload"),
            ProjectField::renamed("key", "parent"),
            ProjectField::named("id"),
        ]);
        let context = GraphBuilder::values(
            RecordDescriptor::new([("account", ValueType::U64), ("unused", ValueType::String)]),
            [vec![Value::U64(9), Value::String("context payload".into())]],
        )
        .unwrap();
        let left = GraphBuilder::join(
            shared.clone().unwrap_nullable("parent"),
            context,
            [] as [&str; 0],
            [] as [&str; 0],
        )
        .project_fields([
            ProjectField::renamed("left.parent", "parent"),
            ProjectField::renamed("right.account", "account"),
            ProjectField::renamed("left.payload", "payload"),
            ProjectField::renamed("left.id", "id"),
        ]);
        let right = GraphBuilder::table("matches");
        let existence = if anti {
            GraphBuilder::anti_join(left, right.clone(), ["parent"], ["key"])
        } else {
            GraphBuilder::semi_join(left, right.clone(), ["parent"], ["key"])
        };
        let graph = existence.project(["account"]);
        let rows = |count| {
            if count == 0 {
                vec![]
            } else {
                vec![(vec![Value::U64(9)], count)]
            }
        };
        let retained = db
            .subscribe([
                ("rows", graph.clone()),
                ("wide", shared),
                ("matches", right),
            ])
            .unwrap();
        let initial = db.next_multisink_subscription(&retained).await.unwrap();
        assert_eq!(
            bag(initial.get("rows").unwrap().to_values().unwrap()),
            rows(if anti { 0 } else { 2 })
        );
        assert_eq!(initial.get("wide").unwrap().deltas.len(), 3);
        assert_eq!(
            first(&mut db, graph.clone()).await,
            rows(if anti { 0 } else { 2 })
        );

        // A discarded payload edit still reaches the independent wide reader.
        let mut batch = db.open_batch();
        batch.update("items", item(1, some(7), "edited payload"));
        db.commit_batch(batch).await.unwrap();
        let delta = db.next_multisink_subscription(&retained).await.unwrap();
        assert!(changes(&delta, "rows").is_empty());
        assert_eq!(
            bag(delta.get("wide").unwrap().to_values().unwrap()),
            bag(vec![
                (
                    vec![
                        Value::String("first payload".into()),
                        some(7),
                        Value::U64(1)
                    ],
                    -1
                ),
                (
                    vec![
                        Value::String("edited payload".into()),
                        some(7),
                        Value::U64(1)
                    ],
                    1
                ),
            ])
        );

        // Two different wide rows became one key with weight two. Retract only one.
        let mut batch = db.open_batch();
        batch.delete("items", PrimaryKeyValue::U64(1));
        db.commit_batch(batch).await.unwrap();
        let delta = db.next_multisink_subscription(&retained).await.unwrap();
        assert_eq!(changes(&delta, "rows"), rows(if anti { 0 } else { -1 }));

        // Right-key multiplicity must remain positive until the last match goes.
        let mut batch = db.open_batch();
        batch.delete("matches", PrimaryKeyValue::U64(10));
        db.commit_batch(batch).await.unwrap();
        let delta = db.next_multisink_subscription(&retained).await.unwrap();
        assert!(changes(&delta, "rows").is_empty());
        let mut batch = db.open_batch();
        batch.delete("matches", PrimaryKeyValue::U64(11));
        batch.insert("matches", vec![Value::U64(12), Value::U64(7)]);
        db.commit_batch(batch).await.unwrap();
        let delta = db.next_multisink_subscription(&retained).await.unwrap();
        assert!(changes(&delta, "rows").is_empty());
        let mut batch = db.open_batch();
        batch.delete("matches", PrimaryKeyValue::U64(12));
        db.commit_batch(batch).await.unwrap();
        let delta = db.next_multisink_subscription(&retained).await.unwrap();
        assert_eq!(changes(&delta, "rows"), rows(if anti { 1 } else { -1 }));
        assert_eq!(
            first(&mut db, graph.clone()).await,
            rows(if anti { 1 } else { 0 })
        );

        // The omitted nullable field still filters; changing null to a value adds a row.
        let mut batch = db.open_batch();
        batch.update("items", item(3, some(8), "null payload"));
        db.commit_batch(batch).await.unwrap();
        let delta = db.next_multisink_subscription(&retained).await.unwrap();
        assert_eq!(changes(&delta, "rows"), rows(if anti { 1 } else { 0 }));
        assert_eq!(first(&mut db, graph).await, rows(if anti { 2 } else { 0 }));
    }
}

#[futures_test::test]
async fn narrow_existence_preserves_array_key_expansion_and_empty_arrays() {
    let mut db = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap();
    let array = |values: &[u64]| Value::Array(values.iter().copied().map(Value::U64).collect());
    let left = GraphBuilder::values(
        RecordDescriptor::new([
            ("id", ValueType::U64),
            ("keys", ValueType::Array(Box::new(ValueType::U64))),
            ("payload", ValueType::String),
        ]),
        [
            vec![Value::U64(1), array(&[7, 7, 8]), Value::String("a".into())],
            vec![Value::U64(1), array(&[7, 7, 8]), Value::String("b".into())],
            vec![Value::U64(2), array(&[]), Value::String("empty".into())],
            vec![
                Value::U64(3),
                array(&[9]),
                Value::String("unmatched".into()),
            ],
        ],
    )
    .unwrap();
    let right = GraphBuilder::values(
        RecordDescriptor::new([("key", ValueType::U64)]),
        [
            vec![Value::U64(7)],
            vec![Value::U64(7)],
            vec![Value::U64(8)],
        ],
    )
    .unwrap();
    let semi =
        GraphBuilder::semi_join(left.clone(), right.clone(), ["keys"], ["key"]).project(["id"]);
    let anti = GraphBuilder::anti_join(left, right, ["keys"], ["key"]).project(["id"]);
    for (graph, expected) in [
        (semi, vec![(vec![Value::U64(1)], 4)]),
        (anti, vec![(vec![Value::U64(3)], 1)]),
    ] {
        assert_eq!(first(&mut db, graph.clone()).await, expected);
        assert_eq!(
            bag(db.query_graph(graph).await.unwrap().to_values().unwrap()),
            expected
        );
    }
}

#[futures_test::test]
async fn projecting_away_the_unwrapped_field_keeps_its_null_filter() {
    let mut db = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap();
    let graph = GraphBuilder::values(
        RecordDescriptor::new([
            ("id", ValueType::U64),
            ("optional", ValueType::Nullable(Box::new(ValueType::U64))),
            ("payload", ValueType::String),
        ]),
        [
            vec![
                Value::U64(1),
                Value::Nullable(None),
                Value::String("absent".into()),
            ],
            vec![Value::U64(2), some(8), Value::String("present".into())],
        ],
    )
    .unwrap()
    .unwrap_nullable("optional")
    .project(["id"]);
    let expected = vec![(vec![Value::U64(2)], 1)];
    assert_eq!(first(&mut db, graph.clone()).await, expected);
    assert_eq!(
        bag(db.query_graph(graph).await.unwrap().to_values().unwrap()),
        expected
    );
}

#[futures_test::test]
async fn narrow_paths_preserve_unnamed_slots_and_nested_projection_dependencies() {
    use groove::records::{DescriptorField, FieldIdentity, OwnedRecord};

    let mut db = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap();
    let details = RecordDescriptor::new([("label", ValueType::String)]);
    let source = GraphBuilder::values(
        RecordDescriptor::new_with_fields([
            DescriptorField {
                name: None,
                identity: Some(FieldIdentity::Slot(42)),
                value_type: ValueType::U64,
            },
            DescriptorField::new("key", ValueType::Nullable(Box::new(ValueType::U64))),
            DescriptorField::new("details", ValueType::Record(Box::new(details))),
            DescriptorField::new("unused", ValueType::String),
        ]),
        [vec![
            Value::U64(900),
            some(7),
            Value::Record(OwnedRecord::new(
                details
                    .create(&[Value::String("nested label".into())])
                    .unwrap(),
                details,
            )),
            Value::String("discarded".into()),
        ]],
    )
    .unwrap()
    .unwrap_nullable("key");
    let right = GraphBuilder::values(
        RecordDescriptor::new([("key", ValueType::U64)]),
        [vec![Value::U64(7)]],
    )
    .unwrap();
    let graph = GraphBuilder::semi_join(source, right, ["key"], ["key"]).project_fields([
        ProjectField::record_field("details", ["label"], "label"),
        ProjectField::renamed_resolved(0, "slot"),
        ProjectField::nullable("key", "nullable_key"),
    ]);
    let expected = vec![(
        vec![
            Value::String("nested label".into()),
            Value::U64(900),
            some(7),
        ],
        1,
    )];
    assert_eq!(first(&mut db, graph.clone()).await, expected);
    assert_eq!(
        bag(db.query_graph(graph).await.unwrap().to_values().unwrap()),
        expected
    );
}

#[futures_test::test]
async fn discarded_fallible_fields_remain_errors_before_existence_and_null_filters() {
    for enum_error in [false, true] {
        for anti in [false, true] {
            let mut db = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
                .await
                .unwrap();
            let tags = ScalarEnumSchema::new("state", ["ok", "bad"]).unwrap();
            let source = GraphBuilder::values(
                RecordDescriptor::new([
                    ("id", ValueType::U64),
                    ("key", ValueType::Nullable(Box::new(ValueType::U64))),
                    ("state", ValueType::EnumTag(tags)),
                ]),
                [vec![
                    Value::U64(1),
                    Value::Nullable(None),
                    Value::EnumTag(1),
                ]],
            )
            .unwrap()
            .project_fields([
                ProjectField::named("id"),
                ProjectField::named("key"),
                if enum_error {
                    ProjectField::enum_tag_remap("state", "bad", vec![Some(0), None])
                } else {
                    ProjectField::literal_typed(
                        "bad",
                        Value::String("not a number".into()),
                        ValueType::U64,
                    )
                },
            ])
            .unwrap_nullable("key");
            let right = GraphBuilder::values(
                RecordDescriptor::new([("key", ValueType::U64)]),
                [] as [Vec<Value>; 0],
            )
            .unwrap();
            let graph = if anti {
                GraphBuilder::anti_join(source, right, ["key"], ["key"])
            } else {
                GraphBuilder::semi_join(source, right, ["key"], ["key"])
            }
            .project(["id"]);
            let sub = db
                .subscribe_with_lifetime([("rows", graph)], SubscriptionLifetime::FirstResult, None)
                .unwrap();
            let error = db.next_multisink_subscription(&sub).await.unwrap_err();
            let expected = if enum_error {
                "enum tag 1 is absent"
            } else {
                "value does not match type U64"
            };
            assert!(error.to_string().contains(expected), "{error}");
        }
    }
}
