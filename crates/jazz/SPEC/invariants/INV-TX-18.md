# INV-TX-18

- Status: now
- Coverage: ✓

## Invariant

Exclusive authority validation MUST reject predicate phantoms by comparing the `(RowUuid, TxId)` output set at `base_snapshot.global_base` against current global output for the same shape and binding.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::exclusive_predicate_phantom_conflict_rejects`; `jazz::node::tests::exclusive_transactions::exclusive_filtered_shape_phantom_conflict_rejects`; `jazz::node::tests::exclusive_transactions::district_scoped_predicate_rejects_same_district_phantom_only`

## Implementation

`jazz/src/node/ingest.rs::NodeState::validate_exclusive_commit_unit`; `jazz/src/node/ingest.rs::NodeState::shape_predicate_changed_after`; `jazz/src/node/ingest.rs::NodeState::shape_output_tx_set_at_global_base`; `jazz/src/node/ingest.rs::NodeState::shape_output_tx_set_now`
