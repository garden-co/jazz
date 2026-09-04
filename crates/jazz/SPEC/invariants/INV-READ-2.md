# INV-READ-2

- Status: now
- Coverage: ✓

## Invariant

A snapshot MUST cover exactly transactions with stored `global_time <= Snapshot.global_base`, transactions from `Snapshot.owner` with `tx_id.time <= Snapshot.local_base`, or transactions explicitly listed in `Snapshot.dots`.

## Enforced by (tests)

`jazz::node::tests::time_travel::snapshot_reads_survive_mid_tx_current_winner_shift`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::snapshot_covers`
