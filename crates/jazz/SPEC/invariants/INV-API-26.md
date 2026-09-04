# INV-API-26

- Status: now
- Coverage: ✓

## Invariant

`Db::mergeable_tx()` MUST group multiple facade writes under one mergeable `TxId`, and the produced commit unit MUST set `Transaction.n_total_writes` to the number of grouped versions.

## Enforced by (tests)

`jazz::db::tests::mergeable_tx_commits_multiple_writes_under_one_tx_id`

## Implementation

`jazz/src/db.rs::Db::mergeable_tx`; `jazz/src/db.rs::MergeableTx::commit`; `jazz/src/node/mod.rs::NodeState::commit_mergeable_many`; `jazz/src/node/mod.rs::NodeState::commit_mergeable_many_at`
