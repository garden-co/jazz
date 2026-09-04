# INV-QUERY-2

- Status: now
- Coverage: ✓

## Invariant

Semantically identical commutative query forms MUST produce the same `ShapeId`; semantic predicate changes MUST produce a different `ShapeId`.

## Enforced by (tests)

`jazz::query::tests::filter_order_does_not_change_shape_id`; `jazz::query::tests::semantic_difference_changes_shape_id`; `jazz::query::tests::canonical_bytes_stability_golden`

## Implementation

`query.rs::normalize_query`; `query.rs::canonical_predicate_key`; `query.rs::canonical_query_bytes`
