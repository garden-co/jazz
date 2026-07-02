use std::collections::BTreeMap;
use std::time::Instant;

mod support;

use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::PeerState;
use jazz::protocol::SyncMessage;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::{DurabilityTier, Fate};
use support::{
    csv_usizes, emit_json_line, insert_durability_tier, insert_node_metrics, phase_fields,
    reset_phase_counters,
};

const TABLE: &str = "todos";

fn main() {
    for depth in depths() {
        for ahead in pending_sizes() {
            let mut bench = ColdSubscriptionBench::new();
            bench.seed_history(depth);
            bench.seed_pending(ahead);

            let global = bench.current_rows_update_elapsed(DurabilityTier::Global);
            let local = bench.current_rows_update_elapsed(DurabilityTier::Local);
            bench.emit_result(depth, ahead, DurabilityTier::Global, global);
            bench.emit_result(depth, ahead, DurabilityTier::Local, local);
        }
    }
}

struct ColdSubscriptionBench {
    writer: NodeState<RocksDbStorage>,
    core: NodeState<RocksDbStorage>,
    _dirs: Vec<tempfile::TempDir>,
}

impl ColdSubscriptionBench {
    fn new() -> Self {
        let schema = schema();
        let mut dirs = Vec::new();
        let (dir, writer) = open_node(node(1), schema.clone());
        dirs.push(dir);
        let (dir, core) = open_node(node(2), schema);
        dirs.push(dir);
        Self {
            writer,
            core,
            _dirs: dirs,
        }
    }

    fn seed_history(&mut self, depth: usize) {
        let row_uuid = row();
        let mut parent = None;
        for idx in 0..depth {
            let mut commit =
                MergeableCommit::new(TABLE, row_uuid, 1_000 + idx as u64).cells(cells(idx));
            if let Some(parent_tx_id) = parent {
                commit = commit.parents(vec![parent_tx_id]);
            }
            let (tx_id, unit) = self
                .writer
                .commit_mergeable_unit(commit)
                .expect("mergeable commit");
            let fate = core_ingest(&mut self.core, &unit, u64::MAX - SKEW_TOLERANCE_MS);
            assert!(matches!(
                fate,
                SyncMessage::FateUpdate {
                    fate: Fate::Accepted,
                    ..
                }
            ));
            parent = Some(tx_id);
        }

        let rows = self
            .core
            .current_rows(TABLE, DurabilityTier::Global)
            .expect("current rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_uuid(), row_uuid);
    }

    fn seed_pending(&mut self, ahead: usize) {
        for idx in 0..ahead {
            self.core
                .commit_mergeable(
                    MergeableCommit::new(TABLE, pending_row(idx), 10_000_000 + idx as u64)
                        .cells(cells(idx)),
                )
                .expect("pending commit");
        }

        let rows = self
            .core
            .current_rows(TABLE, DurabilityTier::Local)
            .expect("current rows");
        assert_eq!(rows.len(), ahead + 1);
    }

    fn current_rows_update_elapsed(&mut self, tier: DurabilityTier) -> std::time::Duration {
        reset_phase_counters(&mut [&mut self.core]);
        let mut peer = PeerState::new();
        let start = Instant::now();
        match tier {
            DurabilityTier::Global => {
                let _ = peer
                    .current_rows_update(&mut self.core, TABLE)
                    .expect("cold global current rows update");
            }
            DurabilityTier::Local => {
                let _ = self
                    .core
                    .current_rows(TABLE, DurabilityTier::Local)
                    .expect("cold local current rows");
            }
            DurabilityTier::None | DurabilityTier::Edge => {
                unreachable!("bench only uses local/global")
            }
        }
        start.elapsed()
    }

    fn emit_result(
        &self,
        depth: usize,
        ahead: usize,
        tier: DurabilityTier,
        elapsed: std::time::Duration,
    ) {
        let phase = match tier {
            DurabilityTier::Global => "global_current_rows_update",
            DurabilityTier::Local => "local_current_rows",
            DurabilityTier::None | DurabilityTier::Edge => {
                unreachable!("bench only uses local/global")
            }
        };
        let mut fields = phase_fields(phase, elapsed.as_micros());
        fields.insert("depth".to_owned(), serde_json::json!(depth));
        fields.insert("pending_ahead".to_owned(), serde_json::json!(ahead));
        insert_durability_tier(&mut fields, tier);
        insert_node_metrics(&mut fields, "core", &self.core);
        emit_json_line("cold_subscription", fields);
    }
}

fn core_ingest(
    core: &mut NodeState<RocksDbStorage>,
    message: &SyncMessage,
    now_ms: u64,
) -> SyncMessage {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    let [fate] = core
        .ingest_commit_unit(tx.clone(), versions.clone(), now_ms)
        .expect("core ingest")
        .try_into()
        .expect("one fate update");
    fate
}

fn depths() -> Vec<usize> {
    csv_usizes("JAZZ_DEPTHS", "1000,5000,10000")
}

fn pending_sizes() -> Vec<usize> {
    csv_usizes("JAZZ_PENDING_SIZES", "0,10,100")
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        TABLE,
        [ColumnSchema::new("title", ColumnType::String)],
    )])
}

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(temp_dir.path(), &refs, Durability::WalNoSync)
            .expect("open rocksdb");
    let node = NodeState::new(node_uuid, schema, storage).expect("single node");
    (temp_dir, node)
}

fn cells(idx: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(format!("title-{idx}")))])
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row() -> RowUuid {
    RowUuid::from_bytes([7; 16])
}

fn pending_row(idx: usize) -> RowUuid {
    let mut bytes = [8; 16];
    bytes[0..8].copy_from_slice(&(idx as u64).to_le_bytes());
    RowUuid::from_bytes(bytes)
}
