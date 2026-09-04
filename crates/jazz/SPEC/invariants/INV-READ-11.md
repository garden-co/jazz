# INV-READ-11

- Status: now
- Coverage: ✓

## Invariant

A local-tier read on the writer node MUST include the node's own pending committed transaction, while a global-tier read MUST exclude it until global fate/current state is applied.

## Enforced by (tests)

`jazz::node::tests::general::writer_subscription_reads_own_pending_at_local_tier`

## Implementation

`jazz/src/node/mod.rs::NodeState::current_rows`; `jazz/src/node/query_eval.rs::NodeState::query_rows_with_prepared_plan`
