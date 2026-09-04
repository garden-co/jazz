# INV-READ-1

- Status: now
- Coverage: ✓

## Invariant

Opening an exclusive transaction on a history-complete core MUST capture its atomically committed `GlobalTime` as `global_base`; a partial node MUST NOT derive a whole-database base from query-scoped receipts.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::core_snapshot_uses_atomically_committed_global_time`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::open_exclusive`; `jazz/src/tx.rs::Snapshot::exclusive_base`
