# INV-QUERY-13

- Status: now
- Coverage: ✓

## Invariant

`tx_query` inside an open exclusive transaction MUST record a binding-sensitive `PredicateRead { shape_id, shape, binding_id, binding_values }`.

## Enforced by (tests)

`jazz::node::tests::exclusive_shape_predicate_is_binding_sensitive`; `jazz::node::tests::exclusive_shape_predicate_validation_uses_inline_shape_without_registration`

## Implementation

`node/open_tx.rs::OpenTx::tx_query`; `tx.rs::PredicateRead`
