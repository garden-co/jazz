//! A routed prepared-shape sink must not lose its delta when the first tick
//! frame parks on a cold read (#3288). Before phase B activates the route
//! barriers, the publication loop may already have published the same
//! subscription's unrouted sink; the routed delta must still arrive.
#![cfg(feature = "test")]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use groove::db::{Database, GraphBuilder, RoutedMultisinkTerminal};
use groove::ivm::ProjectField;
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{TestStorage, TestStorageOperation};

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "docs",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("project_id", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "comments",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("doc_id", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn terminals() -> Vec<RoutedMultisinkTerminal> {
    let bindings = GraphBuilder::binding_source(
        "project_route",
        RecordDescriptor::new([("project_id", ColumnType::U64)]),
    );
    // Routed sink: docs of the bound project joined with their comments, so a
    // doc insert probes the comments side (a cold read after eviction).
    let docs = GraphBuilder::join(
        GraphBuilder::join(
            bindings,
            GraphBuilder::table("docs"),
            ["project_id"],
            ["project_id"],
        )
        .project_fields([
            ProjectField::renamed("right.id", "doc_id"),
            ProjectField::renamed("left.project_id", "__route_project_id"),
        ]),
        GraphBuilder::table("comments"),
        ["doc_id"],
        ["doc_id"],
    )
    .project_fields([
        ProjectField::renamed("left.doc_id", "doc_id"),
        ProjectField::renamed("right.id", "comment_id"),
        ProjectField::renamed("left.__route_project_id", "__route_project_id"),
    ]);
    // Unrouted sink: every doc id, no route fields, so ordinary activation.
    let all = GraphBuilder::table("docs").project_fields([ProjectField::renamed("id", "id")]);
    vec![
        RoutedMultisinkTerminal::new(
            "docs",
            docs,
            ["__route_project_id"],
            ["doc_id", "comment_id"],
        ),
        RoutedMultisinkTerminal::new("all", all, Vec::<String>::new(), ["id"]),
    ]
}

#[test]
fn mixed_routed_and_unrouted_sinks_deliver_both_when_frame_one_parks() {
    let (storage, control) = TestStorage::controlled(&["docs", "comments"]);
    let mut db = block_on(Database::new(schema(), storage.clone())).unwrap();
    let mut seed = db.open_batch();
    seed.insert("comments", vec![Value::U64(100), Value::U64(5)]);
    let applied = block_on(db.apply_batch(seed)).unwrap();
    let persisted = block_on(applied.persist());
    db.finish_persistence(persisted).unwrap();

    let shape = block_on(db.prepare(
        terminals(),
        "project_route",
        RecordDescriptor::new([("project_id", ColumnType::U64)]),
    ))
    .unwrap();
    let sub = block_on(db.bind_shape(shape.id(), &[Value::U64(20)])).unwrap();
    block_on(db.drive_progress()).unwrap();
    while sub.try_recv().is_ok() {}

    storage.evict_all();
    control.take_observed();
    control.pause_on(TestStorageOperation::ScanOpen);
    control.pause_on(TestStorageOperation::Get);
    let mut write = db.open_batch();
    write.insert("docs", vec![Value::U64(5), Value::U64(20)]);
    let mut received = Vec::new();
    {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut apply = Box::pin(db.apply_batch(write));
        let applied = loop {
            match Pin::new(&mut apply).poll(&mut cx) {
                Poll::Ready(result) => break result.unwrap(),
                Poll::Pending => {
                    while let Ok(deltas) = sub.try_recv() {
                        received.push(deltas);
                    }
                    control.resume_operation(TestStorageOperation::ScanOpen);
                    control.resume_operation(TestStorageOperation::Get);
                }
            }
        };
        drop(apply);
        let persisted = block_on(applied.persist());
        db.finish_persistence(persisted).unwrap();
    }
    {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut progress = Box::pin(db.drive_progress());
        for _ in 0..3 {
            match Pin::new(&mut progress).poll(&mut cx) {
                Poll::Ready(result) => {
                    result.unwrap();
                    break;
                }
                Poll::Pending => {
                    while let Ok(deltas) = sub.try_recv() {
                        received.push(deltas);
                    }
                    control.resume_operation(TestStorageOperation::ScanOpen);
                    control.resume_operation(TestStorageOperation::Get);
                }
            }
        }
    }
    block_on(db.drive_progress()).unwrap();
    while let Ok(deltas) = sub.try_recv() {
        received.push(deltas);
    }
    let mut docs = Vec::new();
    let mut all = Vec::new();
    for deltas in &received {
        if let Some(d) = deltas.get("docs") {
            docs.extend(d.to_values().unwrap());
        }
        if let Some(a) = deltas.get("all") {
            all.extend(a.to_values().unwrap());
        }
    }
    assert_eq!(all, [(vec![Value::U64(5)], 1)], "unrouted sink");
    assert_eq!(
        docs,
        [(vec![Value::U64(5), Value::U64(100)], 1)],
        "routed sink must see the new doc/comment pair"
    );
}

mod parked_on_chunk {
    use super::*;
    use bytes::Bytes;
    use groove::chunks::{ChunkRequest, TestChunkProvider};
    use groove::db::PredicateExpr;
    use groove::large_values::{LargeValueKind, prepare};
    use groove::storage::MemoryStorage;
    use std::rc::Rc;

    fn schema() -> DatabaseSchema {
        DatabaseSchema::new([TableSchema::new(
            "docs",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("project_id", ColumnType::U64),
                ColumnSchema::new("body", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
    }

    #[test]
    fn routed_sink_is_not_lost_when_unrouted_sink_publishes_first() {
        let logical = "parked body ".repeat(40_000);
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
        let schema = schema();
        let families = schema
            .column_families()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let mut db = block_on(Database::new(
            schema,
            MemoryStorage::new(&family_refs).unwrap(),
        ))
        .unwrap();
        db.set_chunk_provider(Rc::new(provider));

        let bindings = GraphBuilder::binding_source(
            "project_route",
            RecordDescriptor::new([("project_id", ColumnType::U64)]),
        );
        let docs = GraphBuilder::join(
            bindings,
            GraphBuilder::table("docs"),
            ["project_id"],
            ["project_id"],
        )
        .project_fields([
            ProjectField::renamed("right.id", "id"),
            ProjectField::renamed("right.body", "body"),
            ProjectField::renamed("left.project_id", "__route_project_id"),
        ])
        .filter(PredicateExpr::eq("body", Value::String(logical.clone())))
        .project_fields([
            ProjectField::renamed("id", "id"),
            ProjectField::renamed("__route_project_id", "__route_project_id"),
        ]);
        let all = GraphBuilder::table("docs").project_fields([ProjectField::renamed("id", "id")]);
        let shape = block_on(db.prepare(
            vec![
                RoutedMultisinkTerminal::new("docs", docs, ["__route_project_id"], ["id"]),
                RoutedMultisinkTerminal::new("all", all, Vec::<String>::new(), ["id"]),
            ],
            "project_route",
            RecordDescriptor::new([("project_id", ColumnType::U64)]),
        ))
        .unwrap();
        let sub = block_on(db.bind_shape(shape.id(), &[Value::U64(20)])).unwrap();
        block_on(db.drive_progress()).unwrap();
        while sub.try_recv().is_ok() {}

        control.pause();
        let mut write = db.open_batch();
        write.insert(
            "docs",
            vec![
                Value::U64(5),
                Value::U64(20),
                Value::Large(Box::new(prepared.value_ref)),
            ],
        );
        let mut received = Vec::new();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        {
            let mut apply = Box::pin(db.apply_batch(write));
            let applied = loop {
                match Pin::new(&mut apply).poll(&mut cx) {
                    Poll::Ready(result) => break result.unwrap(),
                    Poll::Pending => {
                        while let Ok(deltas) = sub.try_recv() {
                            received.push(deltas);
                        }
                        control.resume();
                    }
                }
            };
            drop(apply);
            let persisted = block_on(applied.persist());
            db.finish_persistence(persisted).unwrap();
        }
        while let Ok(deltas) = sub.try_recv() {
            received.push(deltas);
        }
        control.resume();
        for _ in 0..5 {
            let mut progress = Box::pin(db.drive_progress());
            loop {
                match Pin::new(&mut progress).poll(&mut cx) {
                    Poll::Ready(result) => {
                        result.unwrap();
                        break;
                    }
                    Poll::Pending => {
                        while let Ok(deltas) = sub.try_recv() {
                            received.push(deltas);
                        }
                        control.resume();
                    }
                }
            }
        }
        while let Ok(deltas) = sub.try_recv() {
            received.push(deltas);
        }
        let mut docs = Vec::new();
        let mut all = Vec::new();
        for deltas in &received {
            if let Some(d) = deltas.get("docs") {
                docs.extend(d.to_values().unwrap());
            }
            if let Some(a) = deltas.get("all") {
                all.extend(a.to_values().unwrap());
            }
        }
        assert_eq!(all, [(vec![Value::U64(5)], 1)], "unrouted sink");
        assert_eq!(docs, [(vec![Value::U64(5)], 1)], "routed sink");
    }
}
