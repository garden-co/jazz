//! Ordering metadata must not hydrate unobserved payloads. Exercise public
//! graphs and real chunk suspension, not a timer-based performance assertion.
#![cfg(feature = "test")]

use bytes::Bytes;
use futures::task::noop_waker;
use groove::chunks::{ChunkRequest, TestChunkProvider, TestChunkProviderControl};
use groove::db::{Database, GraphBuilder, SubscriptionLifetime};
use groove::ivm::{ProjectField, TopByLimit, TopByOrder};
use groove::large_values::{LargeValueKind, full_materializations_for_test, prepare};
use groove::records::{RecordDescriptor, Value, ValueType};
use groove::schema::DatabaseSchema;
use groove::storage::MemoryStorage;
use std::{
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

async fn fixture() -> (Database, GraphBuilder, TestChunkProviderControl, Vec<u8>) {
    let bytes = (0..262_144).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let prepared = prepare(LargeValueKind::Bytes, &bytes).unwrap();
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
    let mut db = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap();
    db.set_chunk_provider(Rc::new(provider));
    let source = GraphBuilder::values(
        RecordDescriptor::new([
            ("id", ValueType::U64),
            ("rank", ValueType::U64),
            ("body", ValueType::Bytes),
        ]),
        [(1, 30), (2, 10), (3, 20)].map(|(id, rank)| {
            vec![
                Value::U64(id),
                Value::U64(rank),
                Value::Large(Box::new(prepared.value_ref.clone())),
            ]
        }),
    )
    .unwrap();
    let ordered = GraphBuilder::top_by(
        source,
        [] as [&str; 0],
        [TopByOrder::asc("rank")],
        ["id"],
        0,
        TopByLimit::Finite(2),
    );
    (db, ordered, control, bytes)
}

#[futures_test::test]
async fn ordered_projection_returns_while_unselected_blob_chunks_are_unavailable() {
    for lifetime in [
        SubscriptionLifetime::FirstResult,
        SubscriptionLifetime::Retained,
    ] {
        let (mut db, ordered, control, _) = fixture().await;
        control.pause();
        let subscription = db
            .subscribe_with_lifetime([("ids", ordered.project(["id"]))], lifetime, None)
            .unwrap();
        let mut next = Box::pin(db.next_multisink_subscription(&subscription));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let Poll::Ready(Ok(rows)) = Pin::new(&mut next).poll(&mut cx) else {
            panic!("an ordered id projection must not wait for an unused blob");
        };
        assert_eq!(
            rows.sinks["ids"].to_values().unwrap(),
            vec![(vec![Value::U64(2)], 1), (vec![Value::U64(3)], 1)]
        );
        assert!(
            control.observed().is_empty(),
            "no omitted payload was requested"
        );
    }
}

#[futures_test::test]
async fn ordered_payload_projection_reconstructs_each_published_blob_only_once() {
    for lifetime in [
        SubscriptionLifetime::FirstResult,
        SubscriptionLifetime::Retained,
    ] {
        let (mut db, ordered, _, bytes) = fixture().await;
        let graph = ordered.project_fields([
            ProjectField::named("id"),
            ProjectField::renamed("body", "payload"),
        ]);
        let before = full_materializations_for_test();
        let subscription = db
            .subscribe_with_lifetime([("files", graph)], lifetime, None)
            .unwrap();
        let rows = db.next_multisink_subscription(&subscription).await.unwrap();
        let rebuilt = full_materializations_for_test() - before;
        assert_eq!(
            rows.sinks["files"].to_values().unwrap(),
            vec![
                (vec![Value::U64(2), Value::Bytes(bytes.clone())], 1),
                (vec![Value::U64(3), Value::Bytes(bytes)], 1),
            ]
        );
        assert_eq!(
            rebuilt, 2,
            "only the two public payloads need reconstruction"
        );
    }
}

#[futures_test::test]
async fn ordering_node_that_is_also_a_public_output_still_hydrates_its_payload() {
    let (mut db, ordered, control, bytes) = fixture().await;
    control.pause();
    let subscription = db
        .subscribe([("wide", ordered.clone()), ("ids", ordered.project(["id"]))])
        .unwrap();
    let mut next = Box::pin(db.next_multisink_subscription(&subscription));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(
        Pin::new(&mut next).poll(&mut cx).is_pending(),
        "a public payload must still wait for its chunks"
    );
    assert!(!control.observed().is_empty());
    control.resume();
    let rows = next.await.unwrap();
    assert_eq!(
        rows.sinks["wide"].to_values().unwrap(),
        vec![
            (
                vec![Value::U64(2), Value::U64(10), Value::Bytes(bytes.clone())],
                1
            ),
            (vec![Value::U64(3), Value::U64(20), Value::Bytes(bytes)], 1),
        ]
    );
    assert_eq!(
        rows.sinks["ids"].to_values().unwrap(),
        vec![(vec![Value::U64(2)], 1), (vec![Value::U64(3)], 1)]
    );
}

#[futures_test::test]
async fn indirect_ordering_identity_is_still_materialized() {
    let key = prepare(LargeValueKind::String, &vec![b'k'; 262_144]).unwrap();
    let chunks = key
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
    let mut db = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap();
    db.set_chunk_provider(Rc::new(provider));
    let physical_key = Value::Large(Box::new(key.value_ref));
    let source = GraphBuilder::values(
        RecordDescriptor::new([("key", ValueType::String), ("rank", ValueType::U64)]),
        [vec![physical_key.clone(), Value::U64(1)]],
    )
    .unwrap();
    let graph = GraphBuilder::top_by(
        source,
        [] as [&str; 0],
        [TopByOrder::asc("rank")],
        ["rank"],
        0,
        TopByLimit::Finite(1),
    )
    .project(["key"]);
    let subscription = db
        .subscribe_with_lifetime_and_root_values(
            [("keys", graph)],
            SubscriptionLifetime::FirstResult,
            groove::ivm::RootIndirectValues::Materialize,
            None,
        )
        .unwrap();
    let mut next = Box::pin(db.next_multisink_subscription(&subscription));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut next).poll(&mut cx).is_pending());
    assert!(!control.observed().is_empty());
    control.resume();
    let rows = next.await.unwrap();
    assert_eq!(
        rows.sinks["keys"].to_values().unwrap(),
        vec![(vec![Value::String("k".repeat(262_144))], 1)]
    );
}
