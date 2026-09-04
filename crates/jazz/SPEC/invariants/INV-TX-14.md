# INV-TX-14

- Status: now
- Coverage: ✓

## Invariant

Exclusive snapshot reads MUST remain stable after later commits and MUST record the read version (including deletion-register versions when deleted) or an absent read.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::exclusive_tx_snapshot_read_ignores_newer_commits_after_open`; `jazz::node::tests::exclusive_transactions::exclusive_tx_snapshot_applies_deletion_register`; `jazz::node::tests::exclusive_transactions::exclusive_absent_read_conflict_rejects`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::tx_read`; `jazz/src/node/open_tx.rs::NodeState::snapshot_row`; `jazz/src/node/open_tx.rs::NodeState::snapshot_layer_winner`
