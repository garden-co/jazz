//! Replay idempotency for transaction fates: a re-delivered identical commit
//! unit returns the already-known fate without reprocessing (INV-TX-4), a
//! re-delivered identical fate update leaves downstream state untouched, and a
//! fate that arrives only after a crash still repairs the interrupted node.

use std::collections::BTreeMap;

use futures::executor::block_on;
use jazz::groove::records::Value;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::PeerState;
use jazz::protocol::SyncMessage;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, RowDescriptor, Schema, TableName,
    TableSchema,
};
use jazz::tx::{DurabilityTier, Fate, TxId};
use jazz_storage_rocksdb::RocksDbStorage;

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

/// `count` uses the counter merge strategy so a reprocessed commit unit would
/// be observable as a doubled delta instead of an unchanged current row.
fn schema() -> JazzSchema {
    let source = Schema::from([(
        TableName::new("tasks"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("count", ColumnType::Integer)
                .merge_strategy(ColumnMergeStrategy::Counter),
        ])),
    )]);
    JazzSchema::new(&source).expect("fate replay public schema compiles")
}

async fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let node = NodeState::new(node_uuid, schema, storage).await.unwrap();
    (temp_dir, node)
}

async fn reopen_node(
    temp_dir: &tempfile::TempDir,
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> NodeState<RocksDbStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    NodeState::new(node_uuid, schema, storage).await.unwrap()
}

fn task_cells(title: &str, count: i32) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("count".to_owned(), Value::I32(count)),
    ])
}

async fn commit(
    client: &mut NodeState<RocksDbStorage>,
    author: AuthorSubject,
    row_uuid: RowUuid,
    made_at: u64,
    title: &str,
    count: i32,
    parents: impl IntoIterator<Item = TxId>,
) -> (TxId, SyncMessage) {
    let (published, message) = client
        .commit_mergeable_unit(
            MergeableCommit::new("tasks", row_uuid, made_at)
                .made_by(author)
                .parents(parents.into_iter().collect())
                .cells(task_cells(title, count)),
        )
        .await
        .unwrap();
    let tx_id = client
        .persist_and_settle_transaction(published)
        .await
        .unwrap();
    (tx_id, message)
}

async fn relay_ingest(node: &mut NodeState<RocksDbStorage>, message: &SyncMessage) {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    node.ingest_relay_commit_unit(tx.clone(), versions.clone())
        .await
        .unwrap();
}

async fn core_ingest(node: &mut NodeState<RocksDbStorage>, message: &SyncMessage) -> SyncMessage {
    let [fate] = core_ingest_all(node, message).await.try_into().unwrap();
    fate
}

async fn core_ingest_all(
    node: &mut NodeState<RocksDbStorage>,
    message: &SyncMessage,
) -> Vec<SyncMessage> {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    let outcome = node
        .ingest_commit_unit(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .await
        .unwrap();
    node.persist_and_settle_outcome(outcome).await.unwrap()
}

async fn apply_message(node: &mut NodeState<RocksDbStorage>, message: SyncMessage) {
    let outcome = node.apply_sync_message(message).await.unwrap();
    node.persist_and_settle_outcome(outcome).await.unwrap();
}

async fn task_rows(
    node: &mut NodeState<RocksDbStorage>,
    tier: DurabilityTier,
) -> Vec<(RowUuid, Value, Value)> {
    let schema = schema();
    let table = &schema.tables[0];
    node.current_rows("tasks", tier)
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.row_uuid(),
                row.cell(table, "title").expect("title"),
                row.cell(table, "count").expect("count"),
            )
        })
        .collect()
}

/// A commit unit delivered twice to the fate authority settles once: the
/// second delivery returns the already-known fate (same fate, same global
/// sequence) and current-row state is unchanged. The counter column makes
/// reprocessing observable: replaying the child's `+3` delta would read 11.
///
/// Actors: alice's client commits, the core is the fate authority.
///
/// ```text
/// alice ──commit seed(count=5)───► core ──fate #1──► Accepted
/// alice ──commit bump(count=8)───► core ──fate #2──► Accepted (delta +3)
/// alice ──redeliver both units───► core ──► same fates, count stays 8
/// ```
#[test]
fn redelivered_identical_commit_unit_returns_known_fate_without_reprocessing() {
    block_on(async {
        let schema = schema();
        let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
        let (_client_dir, mut client) = open_node(node(1), schema.clone()).await;
        let (_core_dir, mut core) = open_node(node(9), schema).await;

        let task_row = row(1);
        let (seed_tx, seed_unit) = commit(&mut client, alice, task_row, 10, "seed", 5, []).await;
        let (bump_tx, bump_unit) =
            commit(&mut client, alice, task_row, 20, "seed", 8, [seed_tx]).await;

        let seed_fate = core_ingest(&mut core, &seed_unit).await;
        let bump_fate = core_ingest(&mut core, &bump_unit).await;
        let settled_rows = task_rows(&mut core, DurabilityTier::Global).await;
        assert_eq!(
            settled_rows,
            vec![(task_row, Value::String("seed".to_owned()), Value::I32(8),)]
        );
        let seed_state = core.transaction_state(seed_tx).await.unwrap();
        let bump_state = core.transaction_state(bump_tx).await.unwrap();
        assert_eq!(seed_state.0, Fate::Accepted);
        assert_eq!(bump_state.0, Fate::Accepted);

        assert_eq!(
            core_ingest_all(&mut core, &seed_unit).await,
            vec![seed_fate],
            "redelivering the seed unit must return its known fate unchanged"
        );
        assert_eq!(
            core_ingest_all(&mut core, &bump_unit).await,
            vec![bump_fate],
            "redelivering the counter bump must return its known fate unchanged"
        );

        assert_eq!(
            task_rows(&mut core, DurabilityTier::Global).await,
            settled_rows,
            "redelivery must not reapply the counter delta"
        );
        assert_eq!(core.transaction_state(seed_tx).await.unwrap(), seed_state);
        assert_eq!(core.transaction_state(bump_tx).await.unwrap(), bump_state);
    });
}

/// A fate update delivered twice downstream is idempotent: after the second
/// delivery the writing client's settled state, pending-upload set, and
/// visible rows are identical to the first delivery, and a relay in between
/// observes the same.
///
/// Actors: alice's client writes, the core decides, a worker relays.
///
/// ```text
/// alice ──commit──► worker(relay) ──► core ──fate──► worker ──► alice
///                                          └─fate (again)─► worker ──► alice
/// ```
#[test]
fn redelivered_fate_update_is_idempotent_downstream() {
    block_on(async {
        let schema = schema();
        let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
        let (_client_dir, mut client) = open_node(node(1), schema.clone()).await;
        let (_worker_dir, mut worker) = open_node(node(2), schema.clone()).await;
        let (_core_dir, mut core) = open_node(node(9), schema).await;

        let task_row = row(2);
        let (tx_id, unit) = commit(&mut client, alice, task_row, 10, "settle me", 5, []).await;
        assert_eq!(
            client
                .pending_transaction_ids_for(node(1), alice)
                .await
                .unwrap(),
            vec![tx_id],
            "the unsettled local write is queued for upload"
        );

        relay_ingest(&mut worker, &unit).await;
        let fate = core_ingest(&mut core, &unit).await;

        for downstream in [&mut client, &mut worker] {
            apply_message(downstream, fate.clone()).await;
        }
        let client_state = client.transaction_state(tx_id).await.unwrap();
        assert_eq!(client_state.0, Fate::Accepted);
        let worker_state = worker.transaction_state(tx_id).await.unwrap();
        let client_rows = task_rows(&mut client, DurabilityTier::Global).await;
        let worker_rows = task_rows(&mut worker, DurabilityTier::Global).await;
        assert!(
            client
                .pending_transaction_ids_for(node(1), alice)
                .await
                .unwrap()
                .is_empty(),
            "a settled fate clears the pending upload set"
        );

        for downstream in [&mut client, &mut worker] {
            apply_message(downstream, fate.clone()).await;
        }
        assert_eq!(
            client.transaction_state(tx_id).await.unwrap(),
            client_state,
            "a redelivered fate must not change the client's settled state"
        );
        assert_eq!(worker.transaction_state(tx_id).await.unwrap(), worker_state);
        assert_eq!(
            task_rows(&mut client, DurabilityTier::Global).await,
            client_rows
        );
        assert_eq!(
            task_rows(&mut worker, DurabilityTier::Global).await,
            worker_rows
        );
        assert!(
            client
                .pending_transaction_ids_for(node(1), alice)
                .await
                .unwrap()
                .is_empty(),
            "a redelivered fate must not requeue the settled transaction"
        );
    });
}

/// A crash between persisting a relayed commit unit and applying its fate is
/// repaired by fate replay: the reopened node still holds the unit as
/// pending, and re-delivering the (duplicated) fate update plus one upstream
/// row refresh restores the settled row.
///
/// Actors: alice's client writes, the core decides, a worker relay crashes.
///
/// ```text
/// alice ──commit──► worker(relay, persists unit) ──✂ crash
///                   core ──fate──► worker(reopened) ──fate again──► repaired
/// ```
#[test]
fn fate_replay_repairs_partially_applied_state() {
    block_on(async {
        let schema = schema();
        let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
        let (_client_dir, mut client) = open_node(node(1), schema.clone()).await;
        let (worker_dir, mut worker) = open_node(node(2), schema.clone()).await;
        let (_core_dir, mut core) = open_node(node(9), schema.clone()).await;

        let task_row = row(3);
        let (tx_id, unit) = commit(&mut client, alice, task_row, 10, "repair me", 5, []).await;

        relay_ingest(&mut worker, &unit).await;
        let fate = core_ingest(&mut core, &unit).await;
        assert_eq!(
            worker.transaction_state(tx_id).await.unwrap().0,
            Fate::Pending
        );

        // The crash window: the unit is persisted but the fate was never applied.
        drop(worker);
        let mut worker = reopen_node(&worker_dir, node(2), schema).await;
        assert_eq!(
            worker.transaction_state(tx_id).await.unwrap().0,
            Fate::Pending,
            "the reopened relay still holds the interrupted unit as pending"
        );

        // Replay delivers the same fate twice; the reapply must be idempotent.
        apply_message(&mut worker, fate.clone()).await;
        apply_message(&mut worker, fate).await;
        let mut core_to_worker = PeerState::new();
        let update = core_to_worker
            .current_rows_update(&mut core, "tasks")
            .await
            .unwrap();
        apply_message(&mut worker, update).await;

        assert_eq!(
            worker.transaction_state(tx_id).await.unwrap().0,
            Fate::Accepted,
            "fate replay settles the interrupted transaction"
        );
        assert_eq!(
            task_rows(&mut worker, DurabilityTier::Global).await,
            vec![(
                task_row,
                Value::String("repair me".to_owned()),
                Value::I32(5),
            )],
            "fate replay repairs the row state the crash interrupted"
        );
    });
}
