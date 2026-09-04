# INV-TX-25

- Status: now
- Coverage: ✓

## Invariant

A local `CommitUnit` is applied as one immediate resident publication before its ordered persistence completes, so same-tick resident queries and subscriptions observe the complete unit. External peer/fate visibility remains withheld until persistence succeeds. A failed or abandoned persistence attempt MUST poison the live runtime rather than retract resident output; durable reopen MUST expose either the whole storage-atomic unit or none of it. Authority ingress MAY defer resident delivery until its atomic durable acceptance succeeds.

## Enforced by (tests)

`jazz::node::tests::harness::failed_multi_row_local_commit_is_fully_resident_but_not_partially_durable`; `jazz::node::tests::harness::authority_storage_failure_returns_no_fate_ack_or_partial_transaction`; `jazz::node::tests::harness::successful_authority_finalization_uses_one_atomic_storage_batch`; `jazz::node::tests::harness::authority_persistence_failure_publishes_no_resident_subscription_output`; `jazz::node::tests::harness::commit_unit_publishes_one_resident_delta_immediately`; `jazz::tests::deferred_local_persistence::cancelled_tick_retains_started_deferred_persistence`; `jazz::tests::deferred_local_persistence::rocksdb_writes_are_resident_before_the_sync_call_returns`

## Implementation

`groove/src/db/mod.rs::Database::apply_batch`; `groove/src/db/mod.rs::AppliedBatch`; `jazz/src/node/state/commit.rs::NodeState::commit_mergeable_many_at_with_schema_versions`; `jazz/src/node/state/commit.rs::NodeState::persist_and_settle_outcome`; `jazz/src/node/ingest/validation.rs::NodeState::ingest_transaction_and_versions`; `jazz/src/node/recovery.rs::NodeState::recover_from_storage`
