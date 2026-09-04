# INV-QUERY-3

- Status: now
- Coverage: ✓

## Invariant

`FilterOp` MUST emit exactly the input deltas whose records satisfy its `PredicateExpr`, preserving record bytes and weights, for the supported predicate surface including `And`/`Or`, literal comparisons, field-to-field equality/inequality, and `Contains`/`ContainsField`.

## Enforced by (tests)

`groove::db::tests::filter_subscriptions_emit_only_matching_rows`

## Implementation

`groove/src/ivm/runtime/mod.rs::NodeState::update_filter`
