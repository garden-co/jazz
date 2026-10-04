//! One pad toggled over and over while offline: reading its current value over
//! a long chain of locally settled edits, each built on the last.
//!
//! Moved from W1 (`w1_local_ahead_current_history`, formerly
//! `examples/benchmarks/w1`), reframed in Wequencer terms: the edited status row
//! is a pad, each candidate an edit. Storage, history shape, timed read and
//! receipt are unchanged.

use std::collections::BTreeMap;

use jazz::block_on;
use jazz::groove::records::Value;
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, TxId};
use jazz_storage_rocksdb::{Durability, RocksDbStorageFactory};

const TABLE: &str = "pads";

/// Pre-seeded candidate history for a single logical current row.
pub struct PadHistoryFixture {
    core: NodeState,
    _directory: tempfile::TempDir,
    depth: usize,
    newest_tx: TxId,
}

impl PadHistoryFixture {
    pub fn new(depth: usize) -> Self {
        assert!(depth > 0, "a pad needs at least one retained edit");
        let schema = schema();
        let directory = tempfile::tempdir().expect("create pad-history fixture directory");
        let storage = block_on(jazz::storage_codec_profile::open_node_storage(
            &RocksDbStorageFactory::with_durability(Durability::WalNoSync),
            directory.path().to_path_buf(),
            schema.column_families(),
        ))
        .expect("admit pad-history RocksDB");
        let mut core =
            block_on(NodeState::new(node(), schema, storage)).expect("open pad-history node");

        let mut parent = None;
        let mut newest_tx = None;
        for index in 0..depth {
            let mut commit =
                MergeableCommit::new(TABLE, row(), 20_000_000 + index as u64).cells(cells(index));
            if let Some(parent_tx) = parent {
                commit = commit.parents(vec![parent_tx]);
            }
            let publication = block_on(core.commit_mergeable(commit)).expect("commit pad edit");
            let tx_id = publication.tx_id();
            block_on(core.persist_and_settle_transaction(publication)).expect("persist pad edit");
            parent = Some(tx_id);
            newest_tx = Some(tx_id);
        }

        Self {
            core,
            _directory: directory,
            depth,
            newest_tx: newest_tx.expect("non-empty pad edit history"),
        }
    }

    /// Untimed correctness receipt for depth, attribution, and winner identity.
    pub fn assert_receipt(&mut self) {
        self.core.reset_storage_read_metrics();
        let rows = self.current_rows();
        let metrics = self.core.storage_read_metrics();

        assert_eq!(
            rows.len(),
            1,
            "{:?} pad winner count",
            DurabilityTier::Local
        );
        assert_eq!(
            rows[0].row_uuid(),
            row(),
            "{:?} pad winner row",
            DurabilityTier::Local
        );
        assert_eq!(
            rows[0].cell_at(0),
            Some(Value::String(state(self.depth - 1))),
            "{:?} the pad must expose its newest edit ({:?})",
            DurabilityTier::Local,
            self.newest_tx,
        );
        assert_eq!(
            metrics.ahead_current_rows.reads,
            self.depth,
            "{:?} the pad read must scan exactly its retained edit depth: {metrics:?}",
            DurabilityTier::Local,
        );
        assert_eq!(
            metrics.ahead_current_rows.ranges,
            2,
            "{:?} the pad read must scan content and deletion ahead-current ranges: {metrics:?}",
            DurabilityTier::Local,
        );
    }

    /// The timed operation: one current-row read over the prepared fixture.
    pub fn current_rows(&mut self) -> Vec<jazz::node::CurrentRow> {
        block_on(self.core.current_rows(TABLE, DurabilityTier::Local)).expect("read current pad")
    }
}

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(TableSchemaBuilder::new(TABLE).column("state", ColumnType::Text))
        .build();
    JazzSchema::new(&source).expect("compile pad-history schema")
}

fn cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([("state".to_owned(), Value::String(state(index)))])
}

fn state(index: usize) -> String {
    format!("pad-{index:011}")
}

fn node() -> NodeUuid {
    NodeUuid::from_bytes([0x71; 16])
}

fn row() -> RowUuid {
    RowUuid::from_bytes([0x17; 16])
}
