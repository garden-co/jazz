# INV-TX-21

- Status: now
- Coverage: ✓

## Invariant

Accepted global transactions MUST maintain per-layer global-current tables/change stream.

## Enforced by (tests)

`jazz::node::tests::sync::accepted_fates_maintain_global_current_tables`

## Implementation

`jazz/src/node/ingest.rs::NodeState::ingest_transaction_and_versions`; `jazz/src/node/ingest.rs::NodeState::write_global_current_update`; `jazz/src/node/ingest.rs::NodeState::apply_fate_update`
