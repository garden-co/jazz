# INV-QUERY-18

- Status: now
- Coverage: ✓

## Invariant

SQL inner joins MUST lower only equality column predicates, with `AND` forming multi-column join keys.

## Enforced by (tests)

`groove::ivm::planner::tests::resolves_qualified_join_keys_and_lowers_inner_join`; `groove::db::tests::query_subscriptions_support_multi_key_inner_joins`

## Implementation

`groove/src/ivm/planner.rs::lower_join_keys`
