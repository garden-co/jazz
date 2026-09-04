# INV-QUERY-14

- Status: now
- Coverage: ✓

## Invariant

Exclusive predicate validation MUST reject an exclusive transaction when the shape/binding output set changed between `base_snapshot.global_base` and validation time, and MUST ignore irrelevant changes outside the shape.

## Enforced by (tests)

`jazz::node::tests::exclusive_filtered_shape_phantom_conflict_rejects`; `jazz::node::tests::exclusive_filtered_shape_ignores_irrelevant_changes`; `jazz::node::tests::filterless_shape_and_degenerate_predicate_validation_agree`

## Implementation

`node/ingest.rs::NodeState::shape_predicate_changed_after`; `node/ingest.rs::NodeState::shape_output_tx_set_now`; `node/ingest.rs::NodeState::shape_output_tx_set_at_global_base`
