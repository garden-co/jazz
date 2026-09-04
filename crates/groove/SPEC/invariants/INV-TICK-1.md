# INV-TICK-1

- Status: now
- Coverage: ✓

## Invariant

A public commit tick MUST advance logical time exactly once and evaluate all durable nodes before evaluating or routing subscription notifications.

## Enforced by (tests)

groove::db::tests::commit_metrics_split_storage_and_tick_work

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::tick_with_params
