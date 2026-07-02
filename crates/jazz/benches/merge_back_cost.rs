use std::collections::BTreeMap;
use std::time::Instant;

mod support;

use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{BranchId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::query::Query;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::DurabilityTier;
use support::{emit_json_line, env_usize, insert_node_metrics, phase_fields, reset_phase_counters};

const TABLE: &str = "todos";

fn main() {
    let writes = env_usize("JAZZ_MERGE_BACK_WRITES", 1_000).max(1);
    let (dir, mut node) = open_node(node_uuid(1), schema());
    let branch_id = branch(0x51);

    reset_phase_counters(&mut [&mut node]);
    let create_start = Instant::now();
    node.create_branch(branch_id).expect("create branch");
    emit_phase("create_branch", create_start.elapsed(), writes, None, &node);

    reset_phase_counters(&mut [&mut node]);
    let write_start = Instant::now();
    for idx in 0..writes {
        node.commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new(TABLE, row(idx), 10 + idx as u64).cells(BTreeMap::from([(
                "title".to_owned(),
                Value::String(format!("branch-{idx}")),
            )])),
        )
        .expect("branch write");
    }
    emit_phase("branch_writes", write_start.elapsed(), writes, None, &node);

    reset_phase_counters(&mut [&mut node]);
    let merge_start = Instant::now();
    node.merge_back_branch(branch_id)
        .expect("merge back branch");
    emit_phase(
        "merge_back_branch",
        merge_start.elapsed(),
        writes,
        None,
        &node,
    );

    reset_phase_counters(&mut [&mut node]);
    let row_count_start = Instant::now();
    let rows = current_row_count(&mut node);
    emit_phase(
        "current_row_count",
        row_count_start.elapsed(),
        writes,
        Some(rows),
        &node,
    );

    drop(dir);
}

fn emit_phase(
    phase: &str,
    elapsed: std::time::Duration,
    writes: usize,
    rows: Option<usize>,
    node: &NodeState<RocksDbStorage>,
) {
    let mut fields = phase_fields(phase, elapsed.as_micros());
    fields.insert("writes".to_owned(), serde_json::json!(writes));
    if let Some(rows) = rows {
        fields.insert("rows".to_owned(), serde_json::json!(rows));
    }
    insert_node_metrics(&mut fields, "node", node);
    emit_json_line("merge_back_cost", fields);
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        TABLE,
        [ColumnSchema::new("title", ColumnType::String)],
    )])
}

fn open_node(uuid: NodeUuid, schema: JazzSchema) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync)
        .expect("rocksdb");
    let node = NodeState::new(uuid, schema, storage).expect("open node");
    (dir, node)
}

fn current_row_count(node: &mut NodeState<RocksDbStorage>) -> usize {
    let shape = Query::from(TABLE).validate(&schema()).expect("query shape");
    node.query_rows(
        &shape,
        &shape.bind(BTreeMap::new()).expect("query binding"),
        DurabilityTier::Local,
    )
    .expect("current rows")
    .len()
}

fn node_uuid(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn branch(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; 16])
}

fn row(idx: usize) -> RowUuid {
    RowUuid::from_bytes((idx as u128).to_be_bytes())
}
