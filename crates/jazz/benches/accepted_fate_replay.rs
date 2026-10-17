//! Isolate the work performed when reconnect re-delivers accepted receipts.
//! TestStorage delegates to the real memory store and counts storage requests;
//! this phase measures core work, not IndexedDB transaction latency.
use std::collections::BTreeMap;
use std::time::Instant;
mod support;
use jazz::groove::{
    records::Value,
    storage::{TestStorage, TestStorageOperation},
};
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::time::GlobalTime;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate};
use serde_json::json;
use support::{BenchFutureExt as _, emit_json_line, env_usize, phase_fields};

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let rows = env_usize("JAZZ_FATE_REPLAY_ROWS", 1_000);
    let repeats = env_usize("JAZZ_FATE_REPLAY_REPEATS", 3);
    let schema = jazz::schema::JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("tasks").column("title", ColumnType::Text))
            .build(),
    )
    .expect("schema");
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&refs);
    let mut replica =
        NodeState::new_with_shared_test_catalogue(NodeUuid::from_bytes([1; 16]), schema, storage)
            .expect("open replica");
    let seed = Instant::now();
    let mut receipts = Vec::with_capacity(rows);
    for index in 0..rows {
        let mut row = [0; 16];
        row[..8].copy_from_slice(&(index as u64).to_le_bytes());
        let (published, _) = replica
            .commit_mergeable_unit(
                MergeableCommit::new("tasks", RowUuid::from_bytes(row), 1000 + index as u64).cells(
                    BTreeMap::from([("title".into(), Value::String(format!("task-{index}")))]),
                ),
            )
            .expect("write row");
        let tx = support::settle_transaction(&mut replica, published);
        let global = GlobalTime(index as u64 + 1);
        replica
            .apply_fate_update(
                tx,
                Fate::Accepted,
                Some(global),
                Some(DurabilityTier::Global),
            )
            .expect("initial acceptance");
        receipts.push((tx, global));
    }
    let seed_us = seed.elapsed().as_micros();
    for pass in 0..repeats {
        control.take_observed();
        replica.reset_storage_read_metrics();
        let start = Instant::now();
        for &(tx, global) in &receipts {
            replica
                .apply_fate_update(
                    tx,
                    Fate::Accepted,
                    Some(global),
                    Some(DurabilityTier::Global),
                )
                .expect("repeat accepted receipt");
        }
        let wall_us = start.elapsed().as_micros();
        let operations = control.take_observed();
        let metrics = replica.storage_read_metrics();
        let mut fields = phase_fields("accepted_receipt_replay", wall_us);
        fields.insert("rows".into(), json!(rows));
        fields.insert("pass".into(), json!(pass));
        fields.insert("seed_us".into(), json!(seed_us));
        fields.insert(
            "write_batches".into(),
            json!(
                operations
                    .iter()
                    .filter(|op| **op == TestStorageOperation::WriteMany)
                    .count()
            ),
        );
        fields.insert(
            "storage_mutations".into(),
            json!(
                operations
                    .iter()
                    .filter(|op| matches!(
                        op,
                        TestStorageOperation::WriteMany
                            | TestStorageOperation::Set
                            | TestStorageOperation::Delete
                    ))
                    .count()
            ),
        );
        fields.insert("point_reads".into(), json!(metrics.total.reads));
        fields.insert("range_reads".into(), json!(metrics.total.ranges));
        emit_json_line("accepted_fate_replay", fields);
    }
}
