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
