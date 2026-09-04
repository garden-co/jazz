# INV-READ-5

- Status: now
- Coverage: ✓

## Invariant

`tx_read` MUST record a `RowRead` for a present snapshot-visible row and an `AbsentRead` for an absent snapshot-visible row.

## Enforced by (tests)

`jazz::node::tests::harness::tx_read_records_present_and_absent_snapshot_reads`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::tx_read`
