use std::rc::Rc;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::task::noop_waker;
use groove::chunks::{ChunkError, ChunkRequest, OwnedChunkProvider, TestChunkProvider};
use groove::db::{Database, GraphBuilder, PredicateExpr};
use groove::ivm::{AggregateExpr, AggregateFunction, ProjectField};
use groove::large_values::{LargeValueKind, TailAppendOutcome, append_tail, prepare};
use groove::records::{RecordDescriptor, Value, ValueType};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

#[futures_test::test]
async fn count_star_does_not_fetch_an_unused_indirect_column() {
    let prepared = prepare(LargeValueKind::String, &vec![b'x'; 800_000]).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let descriptor = RecordDescriptor::new([("body", ValueType::String)]);
    let source = GraphBuilder::values(
        descriptor,
        [
            vec![Value::Large(prepared.value_ref.clone())],
            vec![Value::Large(prepared.value_ref)],
        ],
    )
    .unwrap();
    let graph = GraphBuilder::aggregate(
        source,
        std::iter::empty::<&str>(),
        [AggregateExpr {
            function: AggregateFunction::Count,
            expression: None,
            distinct: false,
            output_name: Some("count".to_owned()),
            output_identity: None,
        }],
    );

    let rows = database
        .query_graph(graph)
        .await
        .unwrap()
        .to_values()
        .unwrap();

    assert_eq!(rows, vec![(vec![Value::U64(2)], 1)]);
    assert!(control.observed().is_empty());
}

#[futures_test::test]
async fn projection_does_not_fetch_an_unselected_indirect_column() {
    let prepared = prepare(LargeValueKind::Bytes, &vec![9; 800_000]).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let descriptor = RecordDescriptor::new([("id", ValueType::U64), ("body", ValueType::Bytes)]);
    let graph = GraphBuilder::values(
        descriptor,
        [vec![Value::U64(7), Value::Large(prepared.value_ref)]],
    )
    .unwrap()
    .project(["id"]);

    let rows = database
        .query_graph(graph)
        .await
        .unwrap()
        .to_values()
        .unwrap();

    assert_eq!(rows, vec![(vec![Value::U64(7)], 1)]);
    assert!(control.observed().is_empty());
}

#[futures_test::test]
async fn filter_does_not_fetch_an_indirect_column_the_predicate_does_not_reference() {
    let prepared = prepare(LargeValueKind::Bytes, &vec![5; 800_000]).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let descriptor = RecordDescriptor::new([("id", ValueType::U64), ("body", ValueType::Bytes)]);
    let graph = GraphBuilder::values(
        descriptor,
        [vec![Value::U64(7), Value::Large(prepared.value_ref)]],
    )
    .unwrap()
    .filter(PredicateExpr::eq("id", Value::U64(7)))
    .project(["id"]);

    let rows = database
        .query_graph(graph)
        .await
        .unwrap()
        .to_values()
        .unwrap();

    assert_eq!(rows, vec![(vec![Value::U64(7)], 1)]);
    assert!(control.observed().is_empty());
}

#[futures_test::test]
async fn join_fetches_only_key_and_selected_large_fields() {
    let prepared = prepare(LargeValueKind::String, &vec![b'z'; 800_000]).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let left = GraphBuilder::values(
        RecordDescriptor::new([("id", ValueType::U64), ("body", ValueType::String)]),
        [vec![Value::U64(7), Value::Large(prepared.value_ref)]],
    )
    .unwrap();
    let right = GraphBuilder::values(
        RecordDescriptor::new([("id", ValueType::U64), ("name", ValueType::String)]),
        [vec![Value::U64(7), Value::String("matched".to_owned())]],
    )
    .unwrap();
    let graph = GraphBuilder::join(left, right, ["id"], ["id"])
        .project_fields([ProjectField::renamed("right.name", "name")]);

    let rows = database
        .query_graph(graph)
        .await
        .unwrap()
        .to_values()
        .unwrap();

    assert_eq!(rows, vec![(vec![Value::String("matched".to_owned())], 1)]);
    assert!(control.observed().is_empty());
}

#[futures_test::test]
async fn subscription_materializes_large_insert_and_update_deltas_atomically() {
    let first_text = "first/".repeat(20_000);
    let second_text = "second/".repeat(20_000);
    let first = prepare(LargeValueKind::String, first_text.as_bytes()).unwrap();
    let second = prepare(LargeValueKind::String, second_text.as_bytes()).unwrap();
    let chunks = first
        .staged_chunks
        .iter()
        .chain(&second.staged_chunks)
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, _) = TestChunkProvider::controlled(chunks);
    let schema = DatabaseSchema::new([TableSchema::new(
        "docs",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let column_families = schema
        .column_families()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut database = Database::new(
        schema,
        MemoryStorage::new(&column_family_refs).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("docs"))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("docs", vec![Value::U64(1), Value::Large(first.value_ref)]);
    let publication = database.apply_batch(batch).await.unwrap();
    database
        .finish_persistence(publication.persist().await)
        .unwrap();
    let inserted = subscription.recv().unwrap().to_values().unwrap();
    assert_eq!(inserted.len(), 1);
    assert_eq!(
        inserted[0].0,
        vec![Value::U64(1), Value::String(first_text.clone())]
    );
    assert_eq!(inserted[0].1, 1);

    let mut batch = database.open_batch();
    batch.update("docs", vec![Value::U64(1), Value::Large(second.value_ref)]);
    let publication = database.apply_batch(batch).await.unwrap();
    database
        .finish_persistence(publication.persist().await)
        .unwrap();
    let updated = subscription.recv().unwrap().to_values().unwrap();
    assert_eq!(updated.len(), 2);
    assert!(updated.contains(&(vec![Value::U64(1), Value::String(first_text)], -1)));
    assert!(updated.contains(&(vec![Value::U64(1), Value::String(second_text)], 1)));
}

#[futures_test::test]
async fn streaming_checksum_subscription_retracts_old_source_and_installs_new_source() {
    let first_bytes = vec![17; 350_000];
    let second_bytes = vec![29; 420_000];
    let first = prepare(LargeValueKind::Bytes, &first_bytes).unwrap();
    let second = prepare(LargeValueKind::Bytes, &second_bytes).unwrap();
    let chunks = first
        .staged_chunks
        .iter()
        .chain(&second.staged_chunks)
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, _) = TestChunkProvider::controlled(chunks);
    let schema = DatabaseSchema::new([TableSchema::new(
        "files",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("body", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let column_families = schema
        .column_families()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut database = Database::new(
        schema,
        MemoryStorage::new(&column_family_refs).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("files").streaming_checksum(
            "body",
            "body_checksum",
            16 * 1024,
            32 * 1024,
        ))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("files", vec![Value::U64(1), Value::Large(first.value_ref)]);
    let publication = database.apply_batch(batch).await.unwrap();
    database
        .finish_persistence(publication.persist().await)
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        vec![(
            vec![
                Value::U64(1),
                Value::Bytes(blake3::hash(&first_bytes).as_bytes().to_vec())
            ],
            1
        )]
    );

    let mut batch = database.open_batch();
    batch.update("files", vec![Value::U64(1), Value::Large(second.value_ref)]);
    let publication = database.apply_batch(batch).await.unwrap();
    database
        .finish_persistence(publication.persist().await)
        .unwrap();
    let updated = subscription.recv().unwrap().to_values().unwrap();
    assert_eq!(updated.len(), 2);
    assert!(updated.contains(&(
        vec![
            Value::U64(1),
            Value::Bytes(blake3::hash(&first_bytes).as_bytes().to_vec())
        ],
        -1
    )));
    assert!(updated.contains(&(
        vec![
            Value::U64(1),
            Value::Bytes(blake3::hash(&second_bytes).as_bytes().to_vec())
        ],
        1
    )));
}

#[futures_test::test]
async fn indirect_string_materializes_as_the_ordinary_logical_query_value() {
    let logical = "large logical text ".repeat(100_000);
    let prepared = prepare(LargeValueKind::String, logical.as_bytes()).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let descriptor = RecordDescriptor::new([("body", ValueType::String)]);
    let graph = GraphBuilder::values(descriptor, [vec![Value::Large(prepared.value_ref)]]).unwrap();

    let rows = database
        .query_graph(graph)
        .await
        .unwrap()
        .to_values()
        .unwrap();

    assert_eq!(rows, vec![(vec![Value::String(logical)], 1)]);
    assert!(
        control.observed().len() > 1,
        "materialization must traverse the indirect tree"
    );
}

#[test]
fn query_future_stays_pending_while_required_chunks_are_paused() {
    futures::executor::block_on(async {
        let logical = "paused logical text ".repeat(30_000);
        let prepared = prepare(LargeValueKind::String, logical.as_bytes()).unwrap();
        let chunks = prepared
            .staged_chunks
            .iter()
            .map(|chunk| {
                (
                    ChunkRequest {
                        object_hash: chunk.node_ref.object_hash.0,
                        locator: chunk.node_ref.locator,
                    },
                    Bytes::copy_from_slice(&chunk.encoded),
                )
            })
            .collect::<Vec<_>>();
        let (provider, control) = TestChunkProvider::controlled(chunks);
        control.pause();
        let mut database = Database::new(
            DatabaseSchema::new([]),
            MemoryStorage::new(&[]).expect("valid memory storage families"),
        )
        .await
        .unwrap();
        database.set_chunk_provider(Rc::new(provider));
        let descriptor = RecordDescriptor::new([("body", ValueType::String)]);
        let graph =
            GraphBuilder::values(descriptor, [vec![Value::Large(prepared.value_ref)]]).unwrap();
        let mut query = Box::pin(database.query_graph(graph));
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut query).poll(&mut context),
            Poll::Pending
        ));
        assert!(!control.observed().is_empty());

        control.resume();
        let rows = query.await.unwrap().to_values().unwrap();
        assert_eq!(rows, vec![(vec![Value::String(logical)], 1)]);
    });
}

#[futures_test::test]
async fn chunk_failure_is_reported_without_publishing_a_partial_result() {
    let logical = "unavailable logical text ".repeat(30_000);
    let prepared = prepare(LargeValueKind::String, logical.as_bytes()).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    control.fail_next(ChunkError::Backend("injected failure".to_owned()));
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let descriptor = RecordDescriptor::new([("body", ValueType::String)]);
    let graph = GraphBuilder::values(descriptor, [vec![Value::Large(prepared.value_ref)]]).unwrap();

    let error = database.query_graph(graph).await.unwrap_err();

    assert!(error.to_string().contains("injected failure"));
}

#[futures_test::test]
async fn indirect_scalars_materialize_inside_composite_values() {
    let logical = b"nested bytes".repeat(50_000);
    let prepared = prepare(LargeValueKind::Bytes, &logical).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, _) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let descriptor = RecordDescriptor::new([(
        "items",
        ValueType::Array(Box::new(ValueType::Nullable(Box::new(ValueType::Bytes)))),
    )]);
    let graph = GraphBuilder::values(
        descriptor,
        [vec![Value::Array(vec![Value::Nullable(Some(Box::new(
            Value::Large(prepared.value_ref),
        )))])]],
    )
    .unwrap();

    let rows = database
        .query_graph(graph)
        .await
        .unwrap()
        .to_values()
        .unwrap();

    assert_eq!(
        rows,
        vec![(
            vec![Value::Array(vec![Value::Nullable(Some(Box::new(
                Value::Bytes(logical),
            )))])],
            1,
        )]
    );
}

#[futures_test::test]
async fn predicates_compare_indirect_strings_by_logical_value() {
    let logical = "predicate text ".repeat(40_000);
    let prepared = prepare(LargeValueKind::String, logical.as_bytes()).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, _) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let descriptor = RecordDescriptor::new([("body", ValueType::String)]);
    let graph = GraphBuilder::values(descriptor, [vec![Value::Large(prepared.value_ref)]])
        .unwrap()
        .filter(PredicateExpr::eq("body", Value::String(logical.clone())));

    let rows = database
        .query_graph(graph)
        .await
        .unwrap()
        .to_values()
        .unwrap();

    assert_eq!(rows, vec![(vec![Value::String(logical)], 1)]);
}

#[futures_test::test]
async fn predicates_compare_present_nullable_indirect_strings_logically() {
    let logical = "nullable predicate text ".repeat(20_000);
    let prepared = prepare(LargeValueKind::String, logical.as_bytes()).unwrap();
    let chunks = prepared.staged_chunks.iter().map(|chunk| {
        (
            ChunkRequest {
                object_hash: chunk.node_ref.object_hash.0,
                locator: chunk.node_ref.locator,
            },
            Bytes::copy_from_slice(&chunk.encoded),
        )
    });
    let (provider, _) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let descriptor =
        RecordDescriptor::new([("body", ValueType::Nullable(Box::new(ValueType::String)))]);
    let graph = GraphBuilder::values(
        descriptor,
        [vec![Value::Nullable(Some(Box::new(Value::Large(
            prepared.value_ref,
        ))))]],
    )
    .unwrap()
    .filter(PredicateExpr::eq("body", Value::String(logical.clone())));

    assert_eq!(
        database
            .query_graph(graph)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(
            vec![Value::Nullable(Some(Box::new(Value::String(logical))))],
            1
        )]
    );
}

#[futures_test::test]
async fn lexical_predicate_stops_chunk_requests_after_decisive_prefix_mismatch() {
    let logical = format!("a{}", "tail".repeat(250_000));
    let literal = format!("z{}", "tail".repeat(250_000));
    let prepared = prepare(LargeValueKind::String, logical.as_bytes()).unwrap();
    let total_chunks = prepared.staged_chunks.len();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let graph = GraphBuilder::values(
        RecordDescriptor::new([("body", ValueType::String)]),
        [vec![Value::Large(prepared.value_ref)]],
    )
    .unwrap()
    .filter(PredicateExpr::gt("body", Value::String(literal)));

    let rows = database
        .query_graph(graph)
        .await
        .unwrap()
        .to_values()
        .unwrap();

    assert!(rows.is_empty());
    assert!(
        control.observed().len() < total_chunks,
        "decisive first-window comparison must not hydrate the remaining tree"
    );
}

#[futures_test::test]
async fn public_consolidation_future_keeps_chunk_suspension_inside_groove() {
    let base = "consolidation base ".repeat(80_000);
    let prepared = prepare(LargeValueKind::String, base.as_bytes()).unwrap();
    let suffix = " appended 😀";
    let TailAppendOutcome::Updated(with_tail) =
        append_tail(&prepared.value_ref, suffix.as_bytes().to_vec()).unwrap()
    else {
        panic!("small append must fit");
    };
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    control.pause();
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    let mut consolidation = Box::pin(database.consolidate_large_value(with_tail));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);

    assert!(matches!(
        Pin::new(&mut consolidation).poll(&mut context),
        Poll::Pending
    ));
    assert!(!control.observed().is_empty());
    control.resume();
    let consolidated = consolidation.await.unwrap();
    let mut expected = base;
    expected.push_str(suffix);
    let fresh = prepare(LargeValueKind::String, expected.as_bytes()).unwrap();

    assert!(consolidated.value_ref.edit_tail.is_empty());
    assert_eq!(
        consolidated.value_ref.logical_hash,
        fresh.value_ref.logical_hash
    );
}

#[futures_test::test]
async fn public_append_preparation_localizes_automatic_tail_consolidation() {
    let base = "append base/".repeat(80_000);
    let original = prepare(LargeValueKind::String, base.as_bytes()).unwrap();
    let mut with_tail = original.value_ref.clone();
    let mut expected = base;
    for _ in 0..groove::large_values::MAX_EDIT_COUNT {
        let suffix = "x";
        let TailAppendOutcome::Updated(updated) =
            append_tail(&with_tail, suffix.as_bytes().to_vec()).unwrap()
        else {
            panic!("tail must fit through its declared count");
        };
        with_tail = updated;
        expected.push_str(suffix);
    }
    let chunks = original
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, _) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));

    let prepared = database
        .append_large_value(with_tail, b"final".to_vec())
        .await
        .unwrap();
    expected.push_str("final");
    let fresh = prepare(LargeValueKind::String, expected.as_bytes()).unwrap();

    assert!(prepared.value_ref.edit_tail.is_empty());
    assert_eq!(
        prepared.value_ref.logical_hash,
        fresh.value_ref.logical_hash
    );
    assert_eq!(prepared.value_ref.byte_length, fresh.value_ref.byte_length);
}

#[futures_test::test]
async fn json_pointer_observes_literal_indirect_json_semantics() {
    let source = br#"{"outer":{"items":[1,{"name":"target"}]},"keep":true}"#;
    let prepared = prepare(LargeValueKind::Json, source).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, _) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));

    assert_eq!(
        database
            .read_large_json_pointer(&prepared.value_ref, "/outer/items/1/name")
            .await
            .unwrap(),
        Some(serde_json::Value::String("target".to_owned()))
    );
    assert_eq!(
        database
            .read_large_json_pointer(&prepared.value_ref, "/missing")
            .await
            .unwrap(),
        None
    );
}

#[futures_test::test]
async fn root_array_json_pointer_stops_after_the_selected_complete_element() {
    let mut source = br#"[{"name":"target"}"#.to_vec();
    for _ in 0..200_000 {
        source.extend_from_slice(br#",{"padding":"xxxxxxxx"}"#);
    }
    source.push(b']');
    let prepared = prepare(LargeValueKind::Json, &source).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let total_chunks = chunks.len();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));

    assert_eq!(
        database
            .read_large_json_pointer(&prepared.value_ref, "/0/name")
            .await
            .unwrap(),
        Some(serde_json::Value::String("target".to_owned()))
    );
    assert!(
        control.observed().len() < total_chunks,
        "early JSON pointer fetched {} of {total_chunks} chunks",
        control.observed().len()
    );
}

#[futures_test::test]
async fn object_json_pointer_preserves_last_duplicate_key_semantics() {
    let source = br#"{"selected":{"value":1},"selected":{"value":2}}"#;
    let prepared = prepare(LargeValueKind::Json, source).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, _) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));
    assert_eq!(
        database
            .read_large_json_pointer(&prepared.value_ref, "/selected/value")
            .await
            .unwrap(),
        Some(serde_json::json!(2))
    );
}

#[futures_test::test]
async fn utf16_ranges_use_tree_metrics_and_tail_coordinates_without_prefix_hydration() {
    let source = "ab😀cd".repeat(160_000);
    let prepared = prepare(LargeValueKind::String, source.as_bytes()).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let total_chunks = chunks.len();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));

    let total = prepared.value_ref.utf16_length.unwrap();
    assert_eq!(
        database
            .read_large_text_utf16_range(&prepared.value_ref, total - 4..total - 2)
            .await
            .unwrap(),
        "😀"
    );
    assert!(
        control.observed().len() < total_chunks,
        "late narrow UTF-16 range fetched {} of {total_chunks} chunks",
        control.observed().len()
    );

    let TailAppendOutcome::Updated(with_tail) =
        append_tail(&prepared.value_ref, "ZZ😀".as_bytes().to_vec()).unwrap()
    else {
        panic!("small append must remain in the tail")
    };
    let tail_total = with_tail.utf16_length.unwrap();
    let before = control.observed().len();
    assert_eq!(
        database
            .read_large_text_utf16_range(&with_tail, tail_total - 2..tail_total)
            .await
            .unwrap(),
        "😀"
    );
    assert_eq!(
        control.observed().len(),
        before,
        "tail-only UTF-16 range fetched base chunks"
    );

    let edited = database
        .edit_large_value(
            prepared.value_ref.clone(),
            (source.len() - 6) as u64,
            "😀".len() as u64,
            "🪩".as_bytes().to_vec(),
        )
        .await
        .unwrap();
    let edited_total = edited.value_ref.utf16_length.unwrap();
    assert_eq!(
        database
            .read_large_text_utf16_range(&edited.value_ref, edited_total - 4..edited_total - 2)
            .await
            .unwrap(),
        "🪩"
    );
}

#[futures_test::test]
async fn utf16_offsets_use_tree_metrics_and_reject_surrogate_interiors() {
    let prefix = "a".repeat(1_200_000);
    let source = format!("{prefix}😀tail");
    let prepared = prepare(LargeValueKind::String, source.as_bytes()).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let total_chunks = chunks.len();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    database.set_chunk_provider(Rc::new(provider));

    let prefix_utf16 = prefix.encode_utf16().count() as u64;
    let prefix_bytes = prefix.len() as u64;
    assert_eq!(
        database
            .large_text_utf16_offset_to_byte(&prepared.value_ref, prefix_utf16)
            .await
            .unwrap(),
        prefix_bytes,
    );
    assert_eq!(
        database
            .large_text_utf16_offset_to_byte(&prepared.value_ref, prefix_utf16 + 2)
            .await
            .unwrap(),
        prefix_bytes + 4,
    );
    assert!(
        database
            .large_text_utf16_offset_to_byte(&prepared.value_ref, prefix_utf16 + 1)
            .await
            .is_err(),
        "a UTF-16 coordinate inside an astral code point must be rejected"
    );
    assert!(
        control.observed().len() < total_chunks,
        "metric-guided offset lookup fetched {} of {total_chunks} chunks",
        control.observed().len()
    );

    let TailAppendOutcome::Updated(with_tail) =
        append_tail(&prepared.value_ref, "x😀".as_bytes().to_vec()).unwrap()
    else {
        panic!("small append must remain in the tail")
    };
    let source_utf16 = source.encode_utf16().count() as u64;
    let source_bytes = source.len() as u64;
    assert_eq!(
        database
            .large_text_utf16_offset_to_byte(&with_tail, source_utf16 + 1)
            .await
            .unwrap(),
        source_bytes + 1,
        "tail coordinates must be resolved in the final logical value"
    );
    assert!(
        database
            .large_text_utf16_offset_to_byte(&with_tail, source_utf16 + 2)
            .await
            .is_err(),
        "tail surrogate interiors must be rejected too"
    );
}

#[futures_test::test]
async fn sequential_cursor_reads_post_edit_logical_value_in_atomic_bounded_windows() {
    let base = (0..1_500_000)
        .map(|index| (index * 17) as u8)
        .collect::<Vec<_>>();
    let prepared = prepare(LargeValueKind::Bytes, &base).unwrap();
    let TailAppendOutcome::Updated(with_tail) =
        append_tail(&prepared.value_ref, b"tail-data".to_vec()).unwrap()
    else {
        panic!("small append must remain in the tail")
    };
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    let owned = OwnedChunkProvider::new_with_budget(Rc::new(provider), 96 * 1024);
    database.set_owned_chunk_provider(owned.clone());
    let mut cursor = groove::large_values::LargeValueCursor::new(with_tail, 31_337).unwrap();

    control.fail_next(ChunkError::Backend("injected".to_owned()));
    assert!(
        database
            .read_large_value_cursor_next(&mut cursor)
            .await
            .is_err()
    );
    assert_eq!(
        cursor.offset(),
        0,
        "failed window advanced cursor publication state"
    );

    let mut actual = Vec::new();
    while let Some(window) = database
        .read_large_value_cursor_next(&mut cursor)
        .await
        .unwrap()
    {
        assert!(window.len() <= 31_337);
        assert_eq!(
            owned.cache_stats().active_leases,
            0,
            "completed cursor window retained its input chunk leases"
        );
        actual.extend(window);
    }
    let mut expected = base;
    expected.extend_from_slice(b"tail-data");
    assert_eq!(actual, expected);
    assert_eq!(cursor.remaining_bytes(), 0);
}

#[futures_test::test]
async fn cached_streaming_checksum_obeys_cooperative_work_budget() {
    let source = (0..600_000)
        .map(|index| (index * 29) as u8)
        .collect::<Vec<_>>();
    let prepared = prepare(LargeValueKind::Bytes, &source).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    let owned = OwnedChunkProvider::new_with_budget(Rc::new(provider), 4 * 1024 * 1024);
    database.set_owned_chunk_provider(owned);

    assert_eq!(
        database
            .read_large_value_range(&prepared.value_ref, 0..source.len() as u64)
            .await
            .unwrap(),
        source
    );
    let provider_reads_after_warmup = control.observed().len();

    let mut checksum =
        Box::pin(database.checksum_large_value_streaming(prepared.value_ref, 16 * 1024, 32 * 1024));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        checksum.as_mut().poll(&mut context),
        Poll::Pending
    ));
    assert_eq!(
        control.observed().len(),
        provider_reads_after_warmup,
        "the first suspension must be the work budget, not backing I/O"
    );

    let mut poll_count = 1;
    let (actual, stats) = loop {
        poll_count += 1;
        if let Poll::Ready(result) = checksum.as_mut().poll(&mut context) {
            break result.unwrap();
        }
    };
    assert_eq!(actual.0, *blake3::hash(&source).as_bytes());
    assert_eq!(stats.logical_bytes_consumed, source.len() as u64);
    assert_eq!(stats.windows_consumed, 37);
    assert!(stats.cooperative_yields >= 18);
    assert_eq!(poll_count, stats.cooperative_yields as usize + 1);
    assert_eq!(control.observed().len(), provider_reads_after_warmup);
}

#[futures_test::test]
async fn graph_streaming_checksum_yields_and_publishes_one_complete_row() {
    let source = (0..600_000)
        .map(|index| (index * 31) as u8)
        .collect::<Vec<_>>();
    let prepared = prepare(LargeValueKind::Bytes, &source).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    let owned = OwnedChunkProvider::new_with_budget(Rc::new(provider), 4 * 1024 * 1024);
    database.set_owned_chunk_provider(owned.clone());
    database
        .read_large_value_range(&prepared.value_ref, 0..source.len() as u64)
        .await
        .unwrap();
    let reads_after_warmup = control.observed().len();

    let descriptor = RecordDescriptor::new([("id", ValueType::U64), ("body", ValueType::Bytes)]);
    let graph = GraphBuilder::values(
        descriptor,
        [vec![Value::U64(7), Value::Large(prepared.value_ref)]],
    )
    .unwrap()
    .streaming_checksum("body", "body_checksum", 16 * 1024, 32 * 1024);
    let mut query = Box::pin(database.query_graph(graph));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(query.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(control.observed().len(), reads_after_warmup);

    let rows = query.await.unwrap().to_values().unwrap();
    assert_eq!(
        rows,
        vec![(
            vec![
                Value::U64(7),
                Value::Bytes(blake3::hash(&source).as_bytes().to_vec())
            ],
            1
        )]
    );
    assert_eq!(control.observed().len(), reads_after_warmup);
    assert_eq!(owned.cache_stats().active_leases, 0);
}

#[futures_test::test]
async fn graph_streaming_checksum_failure_publishes_nothing_and_can_retry() {
    let source = vec![41; 280_000];
    let prepared = prepare(LargeValueKind::Bytes, &source).unwrap();
    let chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect::<Vec<_>>();
    let (provider, control) = TestChunkProvider::controlled(chunks);
    let mut database = Database::new(
        DatabaseSchema::new([]),
        MemoryStorage::new(&[]).expect("valid memory storage families"),
    )
    .await
    .unwrap();
    let owned = OwnedChunkProvider::new_with_budget(Rc::new(provider), 128 * 1024);
    database.set_owned_chunk_provider(owned.clone());
    let descriptor = RecordDescriptor::new([("body", ValueType::Bytes)]);
    let graph = GraphBuilder::values(descriptor, [vec![Value::Large(prepared.value_ref)]])
        .unwrap()
        .streaming_checksum("body", "checksum", 16 * 1024, 32 * 1024);

    control.fail_next(ChunkError::Backend("injected".to_owned()));
    assert!(database.query_graph(graph.clone()).await.is_err());
    assert_eq!(owned.cache_stats().active_leases, 0);

    assert_eq!(
        database
            .query_graph(graph)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(
            vec![Value::Bytes(blake3::hash(&source).as_bytes().to_vec())],
            1
        )]
    );
    assert_eq!(owned.cache_stats().active_leases, 0);
}
