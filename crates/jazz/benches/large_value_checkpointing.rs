use std::time::Instant;

mod support;

use groove::records::Value;
use groove::storage::RocksDbStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{LargeValueEditCommit, NodeState};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::DurabilityTier;
use support::{
    csv_usizes, emit_json_line, insert_node_metrics, phase_fields, reset_phase_counters,
};

fn main() {
    let depth = support::env_usize("JAZZ_LV_DEPTH", 300);
    for interval in csv_usizes("JAZZ_LV_INTERVALS", "64")
        .into_iter()
        .chain([usize::MAX])
    {
        run_case(depth, interval);
    }
}

fn run_case(depth: usize, checkpoint_interval: usize) {
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
    reset_phase_counters(&mut [&mut node]);

    let started = Instant::now();
    let rows = node.current_rows("docs", DurabilityTier::Local).unwrap();
    let elapsed = started.elapsed();
    let bytes = match rows[0].cell(&schema.tables[0], "body") {
        Some(Value::Bytes(bytes)) => bytes.len(),
        other => panic!("expected materialized body bytes, got {other:?}"),
    };
    let mut fields = phase_fields("materialize_current_rows", elapsed.as_micros());
    fields.insert("depth".to_owned(), serde_json::json!(depth));
    fields.insert(
        "checkpoint_interval".to_owned(),
        serde_json::json!(if checkpoint_interval == usize::MAX {
            "full"
        } else {
            "checkpointed"
        }),
    );
    fields.insert(
        "checkpoint_interval_ops".to_owned(),
        serde_json::json!(checkpoint_interval),
    );
    fields.insert("bytes".to_owned(), serde_json::json!(bytes));
    insert_node_metrics(&mut fields, "node", &node);
    emit_json_line("large_value_checkpointing", fields);
}
