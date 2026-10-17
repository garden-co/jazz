//! Internal accounting test: public result rows cannot show whether a parked
//! read unnecessarily keeps requesting graph collection on every wake.
use super::*;
use crate::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use crate::storage::{TestStorage, TestStorageOperation};

#[futures_test::test]
async fn retained_cold_subscription_does_not_keep_gc_pending() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "tasks",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let mut runtime = IvmRuntime::new(schema).unwrap();
    let baseline = runtime.stats().graph_nodes;
    let (storage, control) = TestStorage::controlled(&["tasks"]);
    let storage = Rc::new(storage);
    control.pause_on(TestStorageOperation::ScanOpen);
    let subscription = runtime
        .subscribe(
            [("titles", GraphBuilder::table("tasks").project(["title"]))],
            &storage,
        )
        .unwrap();
    assert!(runtime.pending_incremental.is_pending());
    assert!(subscription.try_recv().is_err());
    runtime.collect_unretained_ephemeral_nodes();
    assert!(
        !runtime.ephemeral_graph_gc_pending,
        "retained graph roots must not keep collection pending just because their reads are queued"
    );
    assert!(runtime.gc_candidates.is_empty());
    control.resume();
    runtime.drive_pending_incremental().await.unwrap();
    assert!(subscription.try_recv().is_ok());
    assert!(runtime.unsubscribe(subscription.id()));
    assert_eq!(runtime.stats().graph_nodes, baseline);
}

/// A cold hydration is the subscription's own work, so unsubscribing cancels
/// it rather than leaving it queued. Nothing may then keep the slice alive or
/// collection pending, and resuming storage must not revive or deliver it.
/// Queued work that outlives its consumer is covered by
/// `pending_incremental_checksum_survives_last_subscription_gc`.
#[futures_test::test]
async fn unsubscribing_during_a_parked_cold_read_releases_the_slice() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "tasks",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let mut runtime = IvmRuntime::new(schema).unwrap();
    let baseline = runtime.stats().graph_nodes;
    let (storage, control) = TestStorage::controlled(&["tasks"]);
    let storage = Rc::new(storage);
    control.pause_on(TestStorageOperation::ScanOpen);
    let subscription = runtime
        .subscribe(
            [("titles", GraphBuilder::table("tasks").project(["title"]))],
            &storage,
        )
        .unwrap();
    assert!(runtime.pending_incremental.is_pending());
    assert!(runtime.stats().graph_nodes > baseline);

    assert!(runtime.unsubscribe(subscription.id()));
    assert!(!runtime.pending_incremental.is_pending());
    assert_eq!(runtime.stats().graph_nodes, baseline);
    assert!(!runtime.ephemeral_graph_gc_pending);
    assert!(runtime.gc_candidates.is_empty());

    control.resume();
    runtime.drive_pending_incremental().await.unwrap();
    assert_eq!(runtime.stats().graph_nodes, baseline);
    assert!(subscription.try_recv().is_err());
}
