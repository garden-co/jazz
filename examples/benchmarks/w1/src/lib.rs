//! W1: hot current reads over retained ahead-current history.
//!
//! The fixture deliberately retains a chain of Local or Edge candidates for a
//! single logical row. Construction and the receipt run before the timed
//! benchmark closure; the closure performs only the current-row read.

use std::collections::BTreeMap;

use jazz::block_on;
use jazz::groove::records::Value;
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate, TxId};
use jazz_storage_rocksdb::{Durability, RocksDbStorage};

const TABLE: &str = "status";

/// Pre-seeded single-row candidate history for one requested visibility tier.
pub struct AheadCurrentFixture {
    core: NodeState<RocksDbStorage>,
    _directory: tempfile::TempDir,
    depth: usize,
    tier: DurabilityTier,
    newest_tx: TxId,
}

impl AheadCurrentFixture {
    pub fn new(depth: usize, tier: DurabilityTier) -> Self {
        assert!(depth > 0, "W1 requires at least one retained candidate");
        assert!(
            matches!(tier, DurabilityTier::Local | DurabilityTier::Edge),
            "W1 only measures Local and Edge candidate visibility"
        );

        let schema = schema();
        let directory = tempfile::tempdir().expect("create W1 fixture directory");
        let families = schema.column_families();
        let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = RocksDbStorage::open_with_durability(
            directory.path(),
            &family_refs,
            Durability::WalNoSync,
        )
        .expect("open W1 RocksDB");
        let mut core = block_on(NodeState::new(node(), schema, storage)).expect("open W1 node");

        let mut parent = None;
        let mut newest_tx = None;
        for index in 0..depth {
            let mut commit =
                MergeableCommit::new(TABLE, row(), 20_000_000 + index as u64).cells(cells(index));
            if let Some(parent_tx) = parent {
                commit = commit.parents(vec![parent_tx]);
            }
            let publication = block_on(core.commit_mergeable(commit)).expect("commit W1 candidate");
            let tx_id = publication.tx_id();
            block_on(core.persist_and_settle_transaction(publication))
                .expect("persist W1 candidate");
            if tier == DurabilityTier::Edge {
                block_on(core.apply_fate_update(
                    tx_id,
                    Fate::Accepted,
                    None,
                    Some(DurabilityTier::Edge),
                ))
                .expect("edge-accept W1 candidate");
            }
            parent = Some(tx_id);
            newest_tx = Some(tx_id);
        }

        Self {
            core,
            _directory: directory,
            depth,
            tier,
            newest_tx: newest_tx.expect("non-empty W1 candidate history"),
        }
    }

    /// Deterministic, untimed W1 receipt. It proves both retained candidate
    /// depth and newest-winner semantics before CodSpeed measures the query.
    pub fn assert_receipt(&mut self) {
        self.core.reset_storage_read_metrics();
        let rows = self.current_rows();
        let metrics = self.core.storage_read_metrics();

        assert_eq!(rows.len(), 1, "{:?} W1 winner count", self.tier);
        assert_eq!(rows[0].row_uuid(), row(), "{:?} W1 winner row", self.tier);
        assert_eq!(
            rows[0].cell_at(0),
            Some(Value::String(title(self.depth - 1))),
            "{:?} W1 must expose the newest candidate ({:?})",
            self.tier,
            self.newest_tx,
        );
        assert_eq!(
            metrics.ahead_current_rows.reads, self.depth,
            "{:?} W1 must read exactly its retained candidate depth: {metrics:?}",
            self.tier,
        );
        assert_eq!(
            metrics.ahead_current_rows.ranges, 2,
            "{:?} W1 must scan content and deletion ahead-current ranges: {metrics:?}",
            self.tier,
        );
    }

    /// The timed W1 operation: one hot current read over the prepared fixture.
    pub fn current_rows(&mut self) -> Vec<jazz::node::CurrentRow> {
        block_on(self.core.current_rows(TABLE, self.tier)).expect("read W1 current rows")
    }
}

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(TableSchemaBuilder::new(TABLE).column("title", ColumnType::Text))
        .build();
    JazzSchema::new(&source).expect("compile W1 schema")
}

fn cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(title(index)))])
}

fn title(index: usize) -> String {
    format!("status-{index:08}")
}

fn node() -> NodeUuid {
    NodeUuid::from_bytes([0x71; 16])
}

fn row() -> RowUuid {
    RowUuid::from_bytes([0x17; 16])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_receipt_reads_exact_local_and_edge_candidate_depth() {
        for tier in [DurabilityTier::Local, DurabilityTier::Edge] {
            AheadCurrentFixture::new(3, tier).assert_receipt();
        }
    }
}
