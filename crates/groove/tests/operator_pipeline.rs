//! Public delivery and cancellation checks for bounded resident unary work.

use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use groove::db::{Database, GraphBuilder, PredicateExpr};
use groove::ivm::ProjectField;
use groove::records::{RecordDescriptor, Value, ValueType};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

fn rows() -> GraphBuilder {
    GraphBuilder::values(
        RecordDescriptor::new([("id", ValueType::U64), ("payload", ValueType::String)]),
        (0..4096).map(|id| vec![Value::U64(id), Value::String(format!("row-{id}"))]),
    )
    .unwrap()
}

#[test]
fn composed_fields_cross_filters_with_exact_nested_nullable_and_constant_payloads() {
    let mut db = block_on(Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).unwrap(),
    ))
    .unwrap();
    let nested = RecordDescriptor::new([("text", ValueType::String)]);
    let descriptor = RecordDescriptor::new([
        ("id", ValueType::U64),
        ("details", ValueType::Record(Box::new(nested))),
    ]);
    let source = GraphBuilder::values(
        descriptor,
        (0..700).map(|id| {
            vec![
                Value::U64(id),
                Value::Record(groove::records::OwnedRecord::new(
                    nested
                        .create(&[Value::String(format!("row-{id}"))])
                        .unwrap(),
                    nested,
                )),
            ]
        }),
    )
    .unwrap();
    let projected = source.project_fields([
        ProjectField::named("id"),
        ProjectField::record_field("details", ["text"], "label"),
        ProjectField::literal("constant", Value::String("row-".into())),
        ProjectField::nullable("id", "optional"),
        ProjectField::null_typed("absent", ValueType::Nullable(Box::new(ValueType::U64))),
    ]);
    let graph = projected
        .clone()
        .filter(PredicateExpr::And(vec![
            PredicateExpr::gt("id", Value::U64(100)),
            PredicateExpr::ContainsField {
                field: "label".into(),
                needle_field: "constant".into(),
            },
            PredicateExpr::EqField {
                field: "id".into(),
                value_field: "optional".into(),
            },
            PredicateExpr::IsNull {
                field: "absent".into(),
            },
        ]))
        .project_fields([
            ProjectField::named("label"),
            ProjectField::nullable("optional", "twice"),
            ProjectField::named("constant"),
            ProjectField::named("absent"),
        ])
        .filter(PredicateExpr::IsNotNull {
            field: "twice".into(),
        });

    // First execution has a private virtual prefix. Then attach an observer,
    // forcing the very same prefix to become a real materialization boundary.
    let first = db.subscribe([("result", graph.clone())]).unwrap();
    let first_result = block_on(db.next_multisink_subscription(&first)).unwrap();
    let observer = db.subscribe([("prefix", projected)]).unwrap();
    assert_eq!(
        block_on(db.next_multisink_subscription(&observer))
            .unwrap()
            .get("prefix")
            .unwrap()
            .deltas
            .len(),
        700
    );
    let second = db.subscribe([("result", graph.clone())]).unwrap();
    let second_result = block_on(db.next_multisink_subscription(&second)).unwrap();
    let one_shot = block_on(db.query_graph(graph)).unwrap();
    let mut expected = (101..700)
        .map(|id| {
            (
                vec![
                    Value::String(format!("row-{id}")),
                    Value::Nullable(Some(Box::new(Value::Nullable(Some(Box::new(Value::U64(
                        id,
                    ))))))),
                    Value::String("row-".into()),
                    Value::Nullable(None),
                ],
                1,
            )
        })
        .collect::<Vec<_>>();
    let sort = |rows: &mut Vec<(Vec<Value>, i64)>| {
        rows.sort_by(|a, b| {
            let (Value::String(a), Value::String(b)) = (&a.0[0], &b.0[0]) else {
                panic!("label")
            };
            a.cmp(b)
        })
    };
    sort(&mut expected);
    for result in [
        first_result.get("result").unwrap(),
        second_result.get("result").unwrap(),
        &one_shot,
    ] {
        let mut actual = result.to_values().unwrap();
        sort(&mut actual);
        assert_eq!(actual, expected);
    }
}

#[test]
fn discarded_invalid_constant_is_not_hidden_by_a_virtual_filter() {
    let mut db = block_on(Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).unwrap(),
    ))
    .unwrap();
    let graph = rows()
        .project_fields([
            ProjectField::named("id"),
            ProjectField::literal_typed(
                "bad",
                Value::String("not a number".into()),
                ValueType::U64,
            ),
        ])
        .filter(PredicateExpr::gt("id", Value::U64(99999)))
        .project(["id"]);
    let subscription = db.subscribe([("result", graph)]).unwrap();
    let error = block_on(db.next_multisink_subscription(&subscription)).unwrap_err();
    assert!(
        error.to_string().contains("value does not match type U64"),
        "{error}"
    );
}

#[test]
fn resident_unary_batches_yield_without_partial_results_and_preserve_shared_outputs() {
    let mut db = block_on(Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).unwrap(),
    ))
    .unwrap();
    let input = rows();
    let graph = input
        .clone()
        .filter(PredicateExpr::gt("id", Value::U64(100)))
        .project(["id"]);
    let waker = noop_waker();
    let subscription = db
        .subscribe_with_waker([("selected", graph.clone()), ("all", input)], Some(&waker))
        .unwrap();
    let mut cx = Context::from_waker(&waker);
    let mut yields = 0;
    let result = loop {
        match db.poll_multisink_subscription(&subscription, &mut cx) {
            Poll::Pending => {
                yields += 1;
                assert!(
                    yields < 100,
                    "resident work must make bounded forward progress"
                );
                assert!(
                    subscription.try_recv().is_err(),
                    "no partial snapshot is published"
                );
            }
            Poll::Ready(result) => break result.unwrap(),
        }
    };
    assert!(
        yields > 1,
        "large resident unary work must yield between batches"
    );
    let mut actual = result.get("selected").unwrap().to_values().unwrap();
    actual.sort_by_key(|(row, _)| match row[0] {
        Value::U64(id) => id,
        _ => panic!("id"),
    });
    let expected = (101..4096)
        .map(|id| (vec![Value::U64(id)], 1))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    let mut all = result.get("all").unwrap().to_values().unwrap();
    all.sort_by_key(|(row, _)| match row[0] {
        Value::U64(id) => id,
        _ => panic!("id"),
    });
    assert_eq!(
        all,
        (0..4096)
            .map(|id| (vec![Value::U64(id), Value::String(format!("row-{id}"))], 1))
            .collect::<Vec<_>>()
    );
    let mut one_shot = block_on(db.query_graph(graph))
        .unwrap()
        .to_values()
        .unwrap();
    one_shot.sort_by_key(|(row, _)| match row[0] {
        Value::U64(id) => id,
        _ => panic!("id"),
    });
    assert_eq!(one_shot, expected);
}

#[test]
fn cancelling_a_yielded_projection_does_not_publish_its_partial_prefix() {
    let mut db = block_on(Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).unwrap(),
    ))
    .unwrap();
    let graph = rows().project(["id"]);
    let waker = noop_waker();
    let subscription = db
        .subscribe_with_waker([("ids", graph.clone())], Some(&waker))
        .unwrap();
    let mut cx = Context::from_waker(&waker);
    assert!(
        db.poll_multisink_subscription(&subscription, &mut cx)
            .is_pending()
    );
    assert!(subscription.try_recv().is_err());
    assert!(db.unsubscribe(subscription.id()));
    let replacement = db.subscribe([("ids", graph)]).unwrap();
    let result = block_on(db.next_multisink_subscription(&replacement)).unwrap();
    let mut actual = result.get("ids").unwrap().to_values().unwrap();
    actual.sort_by_key(|(row, _)| match row[0] {
        Value::U64(id) => id,
        _ => panic!("id"),
    });
    assert_eq!(
        actual,
        (0..4096)
            .map(|id| (vec![Value::U64(id)], 1))
            .collect::<Vec<_>>()
    );
}

#[test]
fn a_late_projection_error_discards_the_completed_prefix_even_if_later_filtered_out() {
    let mut db = block_on(Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).unwrap(),
    ))
    .unwrap();
    let descriptor = RecordDescriptor::new([
        ("id", ValueType::U64),
        (
            "tag",
            ValueType::EnumTag(
                groove::records::ScalarEnumSchema::new("status", ["kept", "absent"]).unwrap(),
            ),
        ),
    ]);
    let graph = GraphBuilder::values(
        descriptor,
        (0..2049).map(|id| vec![Value::U64(id), Value::EnumTag(u8::from(id == 2048))]),
    )
    .unwrap()
    .project_fields([
        ProjectField::named("id"),
        ProjectField::enum_tag_remap("tag", "tag", vec![Some(0), None]),
    ])
    .filter(PredicateExpr::Lt {
        field: "id".into(),
        value: Value::U64(2048).into(),
    });
    let waker = noop_waker();
    let subscription = db
        .subscribe_with_waker([("result", graph)], Some(&waker))
        .unwrap();
    let mut cx = Context::from_waker(&waker);
    let mut yields = 0;
    let error = loop {
        match db.poll_multisink_subscription(&subscription, &mut cx) {
            Poll::Pending => {
                yields += 1;
                assert!(yields < 30);
                assert!(subscription.try_recv().is_err());
            }
            Poll::Ready(result) => break result.unwrap_err(),
        }
    };
    assert!(yields > 1);
    assert!(
        error
            .to_string()
            .contains("enum tag 1 is absent from this projection target"),
        "{error}"
    );
}

#[test]
fn batched_retractions_crossing_filter_boundary_reach_both_subscribers_exactly() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("rank", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let mut db = block_on(Database::new(
        schema,
        MemoryStorage::new(&["items"]).unwrap(),
    ))
    .unwrap();
    let graph = GraphBuilder::table("items")
        .filter(PredicateExpr::gt("rank", Value::U64(0)))
        .project(["id", "payload"]);
    let first = block_on(db.subscribe_one_sink(graph.clone())).unwrap();
    let second = block_on(db.subscribe_one_sink(graph)).unwrap();
    assert!(first.recv().unwrap().is_empty());
    assert!(second.recv().unwrap().is_empty());
    for step in 0..3 {
        let mut batch = db.open_batch();
        for id in 0..700 {
            if step == 0 {
                batch.insert(
                    "items",
                    vec![
                        Value::U64(id),
                        Value::U64(1),
                        Value::String(format!("row-{id}")),
                    ],
                );
            } else {
                batch.update(
                    "items",
                    vec![
                        Value::U64(id),
                        Value::U64(if step == 1 { 0 } else { 1 }),
                        Value::String(format!("row-{id}")),
                    ],
                );
            }
        }
        let publication = block_on(db.apply_batch(batch)).unwrap();
        let persistence = block_on(publication.persist());
        db.finish_persistence(persistence).unwrap();
        let expected = (0..700)
            .map(|id| {
                (
                    vec![Value::U64(id), Value::String(format!("row-{id}"))],
                    if step == 1 { -1 } else { 1 },
                )
            })
            .collect::<Vec<_>>();
        for subscription in [&first, &second] {
            let mut actual = block_on(db.next_subscription(subscription))
                .unwrap()
                .to_values()
                .unwrap();
            actual.sort_by_key(|(row, _)| match row[0] {
                Value::U64(id) => id,
                _ => panic!("id"),
            });
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn fused_stages_preserve_upstream_error_precedence_across_rows_and_yields() {
    let mut db = block_on(Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).unwrap(),
    ))
    .unwrap();
    let tags = groove::records::ScalarEnumSchema::new("status", ["ok", "early", "late"]).unwrap();
    let graph = GraphBuilder::values(
        RecordDescriptor::new([
            ("id", ValueType::U64),
            ("upstream", ValueType::EnumTag(tags.clone())),
            ("downstream", ValueType::EnumTag(tags)),
        ]),
        (0..2049).map(|id| {
            vec![
                Value::U64(id),
                Value::EnumTag(if id == 2048 { 2 } else { 0 }),
                Value::EnumTag(u8::from(id == 0)),
            ]
        }),
    )
    .unwrap()
    .project_fields([
        ProjectField::named("id"),
        ProjectField::enum_tag_remap("upstream", "upstream", vec![Some(0), Some(1), None]),
        ProjectField::named("downstream"),
    ])
    .filter(PredicateExpr::Or(vec![
        PredicateExpr::gt("id", Value::U64(0)),
        PredicateExpr::eq("id", Value::U64(0)),
    ]))
    .project_fields([
        ProjectField::named("id"),
        ProjectField::enum_tag_remap("downstream", "downstream", vec![Some(0), None, Some(2)]),
    ]);
    let waker = noop_waker();
    let subscription = db
        .subscribe_with_waker([("result", graph)], Some(&waker))
        .unwrap();
    let mut cx = Context::from_waker(&waker);
    let mut yields = 0;
    let error = loop {
        match db.poll_multisink_subscription(&subscription, &mut cx) {
            Poll::Pending => {
                yields += 1;
                assert!(yields < 40);
                assert!(subscription.try_recv().is_err());
            }
            Poll::Ready(result) => break result.unwrap_err(),
        }
    };
    assert!(yields > 1);
    assert!(
        error.to_string().contains("enum tag 2 is absent"),
        "{error}"
    );
}

#[test]
fn adding_an_observer_to_a_previously_private_prefix_preserves_both_outputs() {
    let mut db = block_on(Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).unwrap(),
    ))
    .unwrap();
    let prefix = rows().filter(PredicateExpr::gt("id", Value::U64(100)));
    let tail = prefix.clone().project(["id"]);
    let first = block_on(db.subscribe_one_sink(tail.clone())).unwrap();
    assert_eq!(first.recv().unwrap().deltas.len(), 3995);
    let observer = block_on(db.subscribe_one_sink(prefix)).unwrap();
    let all = observer.recv().unwrap().to_values().unwrap();
    assert_eq!(all.len(), 3995);
    for (row, weight) in all {
        let Value::U64(id) = row[0] else {
            panic!("id");
        };
        assert_eq!(weight, 1);
        assert!(id > 100);
        assert_eq!(row[1], Value::String(format!("row-{id}")));
    }
    let again = block_on(db.subscribe_one_sink(tail)).unwrap();
    let mut ids = again.recv().unwrap().to_values().unwrap();
    ids.sort_by_key(|(row, _)| match row[0] {
        Value::U64(id) => id,
        _ => panic!("id"),
    });
    assert_eq!(
        ids,
        (101..4096)
            .map(|id| (vec![Value::U64(id)], 1))
            .collect::<Vec<_>>()
    );
}
