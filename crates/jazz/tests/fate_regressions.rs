//! Fate finality: `accepted`/`rejected` never changes once assigned
//! (IMPLEMENTATION.md invariant). The fuzz driver duplicates and reorders
//! fate updates by default, so a stale `pending` arriving after `accepted`
//! must be ignored or rejected — never applied.

use std::collections::BTreeMap;

use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::{DurabilityTier, Fate};

use groove::schema::{ColumnSchema, ColumnType};
use groove::storage::RocksDbStorage;

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )])
}

fn open_node(byte: u8) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let node = NodeState::new(NodeUuid::from_bytes([byte; 16]), schema, storage).unwrap();
    (temp_dir, node)
}

#[test]
fn stale_pending_fate_update_cannot_regress_accepted() {
    let (_dir, mut client) = open_node(1);
    let (tx_id, _unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new("todos", RowUuid::from_bytes([7; 16]), 10)
                .cells(BTreeMap::from([("title".to_owned(), "x".to_owned())])),
        )
        .unwrap();

    client
        .apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(jazz::time::GlobalSeq(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();

    // A duplicated/reordered stale fate update arrives afterwards.
    let _ = client.apply_fate_update(tx_id, Fate::Pending, None, Some(DurabilityTier::Global));

    let (fate, global_seq, durability) = client.transaction_state(tx_id).unwrap();
    assert_eq!(
        fate,
        Fate::Accepted,
        "fate is final: accepted must never regress to pending"
    );
    assert_eq!(global_seq, Some(jazz::time::GlobalSeq(1)));
    assert_eq!(durability, DurabilityTier::Global);
}
