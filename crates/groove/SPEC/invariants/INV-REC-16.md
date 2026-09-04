# INV-REC-16

- Status: now
- Coverage: ✓

## Invariant

A terminal collector's touched-rendered-group bound MUST NOT be applied to recursive fixed-point state, iterations, or nested logical time.

## Enforced by (tests)

`groove::db::tests::collect_by_after_recursive_closure_keeps_recursive_state_outside_limit`

## Implementation

`groove/src/ivm/runtime/mod.rs::TickEvaluator::update_collect_by`; `groove/src/ivm/runtime/mod.rs::validate_collect_by_terminality`
