use std::time::Instant;

use groove::records::Value;
use groove::storage::RocksDbStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{LargeValueEditCommit, NodeState};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::DurabilityTier;

fn main() {
    let depth = 300usize;
    let checkpoint_interval = 64usize;
    let checkpointed = run_case(depth, checkpoint_interval);
    let full = run_case(depth, usize::MAX);

    println!(
        "large_value_checkpointing depth={depth} checkpoint_interval={checkpoint_interval} \
         checkpointed_ms={} checkpointed_replayed_ops={} checkpointed_hits={} checkpointed_bytes={} \
         full_ms={} full_replayed_ops={} full_hits={} full_bytes={}",
        checkpointed.elapsed_ms,
        checkpointed.replayed_ops,
        checkpointed.checkpoint_hits,
        checkpointed.bytes,
        full.elapsed_ms,
        full.replayed_ops,
        full.checkpoint_hits,
        full.bytes
    );
}

struct CaseResult {
    elapsed_ms: u128,
    replayed_ops: usize,
    checkpoint_hits: u64,
    bytes: usize,
}

fn run_case(depth: usize, checkpoint_interval: usize) -> CaseResult {
    let schema = JazzSchema::new([TableSchema::new(
        "docs",
        [jazz::schema::ColumnSchema::text("body")],
    )]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let mut node = NodeState::new_with_large_value_checkpoint_op_interval(
        NodeUuid::from_bytes([0x42; 16]),
        schema.clone(),
        storage,
        true,
        checkpoint_interval,
    )
    .unwrap();
    let row_uuid = RowUuid::from_bytes([0x24; 16]);
    for idx in 0..depth {
        let tx_id = node
            .commit_large_value_edit(
                LargeValueEditCommit::new("docs", row_uuid, "body", 10 + idx as u64)
                    .made_by(AuthorId::from_bytes([0xa1; 16]))
                    .insert(idx, b"x"),
            )
            .unwrap();
        node.finalize_local_mergeable_commit(tx_id).unwrap();
    }
    node.reset_large_value_metrics();

    let started = Instant::now();
    let rows = node.current_rows("docs", DurabilityTier::Local).unwrap();
    let elapsed_ms = started.elapsed().as_millis();
    let bytes = match rows[0].cell(&schema.tables[0], "body") {
        Some(Value::Bytes(bytes)) => bytes.len(),
        other => panic!("expected materialized body bytes, got {other:?}"),
    };
    let metrics = node.large_value_metrics();
    CaseResult {
        elapsed_ms,
        replayed_ops: metrics.last_replayed_ops,
        checkpoint_hits: metrics.checkpoint_hits,
        bytes,
    }
}
