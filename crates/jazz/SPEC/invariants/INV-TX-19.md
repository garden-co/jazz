# INV-TX-19

- Status: now
- Coverage: ✓

## Invariant

Exclusive predicate validation MUST be sensitive to `binding_id`/`binding_values` and MUST use the inline query shape without requiring prior shape registration.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::exclusive_shape_predicate_is_binding_sensitive`; `jazz::node::tests::exclusive_transactions::exclusive_shape_predicate_validation_uses_inline_shape_without_registration`

## Implementation

`jazz/src/node/query_eval.rs::NodeState::tx_query`; `jazz/src/node/ingest.rs::NodeState::shape_predicate_changed_after`
