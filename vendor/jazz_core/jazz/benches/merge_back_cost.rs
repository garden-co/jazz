use std::collections::BTreeMap;
use std::env;
use std::time::Instant;

use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{BranchId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::query::Query;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::DurabilityTier;

const TABLE: &str = "todos";

fn main() {
    let writes = env_usize("JAZZ_MERGE_BACK_WRITES", 1_000).max(1);
    let (dir, mut node) = open_node(node_uuid(1), schema());
    let branch_id = branch(0x51);

    let create_start = Instant::now();
    node.create_branch(branch_id).expect("create branch");
    let create_ms = create_start.elapsed().as_millis();

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
    let write_ms = write_start.elapsed().as_millis();

    let merge_start = Instant::now();
    node.merge_back_branch(branch_id)
        .expect("merge back branch");
    let merge_ms = merge_start.elapsed().as_millis();
    let rows = current_row_count(&mut node);

    println!(
        "{{\"writes\":{writes},\"rows\":{rows},\"create_ms\":{create_ms},\"branch_write_ms\":{write_ms},\"merge_back_ms\":{merge_ms}}}"
    );
    drop(dir);
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

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
