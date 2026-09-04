# INV-TICK-8

- Status: now
- Coverage: ✓

## Invariant

Arrangement state MUST NOT move backward in logical time; stale reads MUST fail instead of returning data for the wrong `Tick`/`SubTick`.

## Enforced by (tests)

`groove::ivm::runtime::tests::stale_as_of_state_rejects_wrong_or_backward_logical_time`

## Implementation

groove/src/ivm/runtime/mod.rs::AsOf::value_at; groove/src/ivm/runtime/mod.rs::AsOf::mark_forward_as_of; groove/src/ivm/runtime/join.rs::advance_arrangement
