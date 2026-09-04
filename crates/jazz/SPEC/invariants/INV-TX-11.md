# INV-TX-11

- Status: now
- Coverage: ✓

## Invariant

Accepted core commits MUST receive a strictly increasing authority-minted `GlobalTime`; accepted state and `committed_global_time` MUST become durable atomically before the authority publishes a fate or exposes the commit externally.

## Enforced by (tests)

`jazz::node::tests::sync::observed_global_time_advances_authority_allocator`; `jazz::node::tests::sync::commit_units_sync_upstream_and_fates_flow_back`; `jazz::node::tests::harness::authority_storage_failure_returns_no_fate_ack_or_partial_transaction`; `jazz::node::tests::harness::successful_authority_finalization_uses_one_atomic_storage_batch`

## Implementation

`jazz/src/node/ingest.rs::NodeState::ingest_commit_unit_once`; `jazz/src/node/open_tx.rs::NodeState::record_applied_global_time`; `jazz/src/node/state/commit.rs::NodeState::persist_and_settle_outcome`
