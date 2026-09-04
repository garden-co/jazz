# INV-TX-13

- Status: now
- Coverage: ✓

## Invariant

A core exclusive transaction MUST capture the core's atomically committed `GlobalTime`; a partial node MUST NOT promote query-scoped settlement into a whole-database global base.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::core_snapshot_uses_atomically_committed_global_time`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::open_exclusive`; `jazz/src/node/open_tx.rs::NodeState::record_applied_global_time`
