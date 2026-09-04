# INV-REC-13

- Status: now
- Coverage: ✓

## Invariant

Jazz MUST reject user-authored logical operations that would lower to `arg_max_by` or `arg_min_by` inside a recursive seed or step before resolving physical current-row sources. Physical ArgBy introduced by that source expansion MAY occur inside recursion and MUST use the same declared-key direction and ascending full-record tie-breaker as non-recursive evaluation before its winners enter recursive set accumulation.

## Enforced by (tests)

`jazz::node::query_engine::tests::recursive_relation_has_explicit_recursive_plan_and_relation_facts`; `groove::db::tests::arg_by_snapshot_hydration_tie_breaker_is_independent_of_reversed_input_order`

## Implementation

`jazz/src/node/query_engine/lowering/planning.rs::validate_recursive_arg_by_capabilities`; `groove/src/ivm/runtime/recursion.rs::HydrationEvaluator::eval_node`; `groove/src/ivm/runtime/windows.rs::arg_by_candidate_replaces`
