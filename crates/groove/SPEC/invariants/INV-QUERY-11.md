# INV-QUERY-11

- Status: now
- Coverage: ✓

## Invariant

Shared join arrangements MUST apply a given logical-time delta at most once per arrangement key/scope, even when multiple joins consume the arrangement.

## Enforced by (tests)

`groove::tests::arrangement_regressions::sibling_joins_sharing_an_arrangement_do_not_double_count`; `groove::tests::arrangement_regressions::arrangement_shared_across_sub_ticks_is_applied_once_per_tick`

## Implementation

`groove/src/ivm/runtime/join.rs::advance_arrangement`
