# INV-READ-3

- Status: now
- Coverage: ✓

## Invariant

Reads inside an open exclusive transaction MUST choose the domination winner among snapshot-covered versions per `VersionLayer` and MUST NOT observe later uncovered current-winner changes.

## Enforced by (tests)

`jazz::node::tests::time_travel::snapshot_reads_survive_mid_tx_current_winner_shift`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::snapshot_layer_winner`
