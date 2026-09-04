# INV-READ-4

- Status: now
- Coverage: ✓

## Invariant

Reads inside an open exclusive transaction MUST overlay that transaction's own pending writes on top of the snapshot-covered base view.

## Enforced by (tests)

`jazz::node::tests::harness::exclusive_tx_pending_writes_overlay_snapshot_for_point_and_table_reads`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::overlay_pending_writes`
