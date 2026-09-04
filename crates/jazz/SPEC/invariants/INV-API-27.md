# INV-API-27

- Status: now
- Coverage: ✓

## Invariant

`Db::exclusive_tx()` MUST expose serializable exclusive transactions on the facade, preserving snapshot reads and returning `WriteRejected` when authority validation detects a conflict.

## Enforced by (tests)

`jazz::db::tests::exclusive_tx_rejects_conflicting_concurrent_update`; `jazz::db::tests::exclusive_tx_blind_writes_are_first_committer_wins`

## Implementation

`jazz/src/db.rs::Db::exclusive_tx`; `jazz/src/db.rs::ExclusiveTx::commit`; `jazz/src/node/open_tx.rs::NodeState::commit_exclusive`; `jazz/src/node/ingest.rs::NodeState::validate_exclusive_commit_unit`
