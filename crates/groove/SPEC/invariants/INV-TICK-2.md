# INV-TICK-2

- Status: now
- Coverage: ✓

## Invariant

A subscription MUST receive exactly one initial hydration `RecordDeltas` message, including an empty message for an empty result, before it receives future commit deltas.

## Enforced by (tests)

groove::db::tests::subscribe_sends_empty_hydration_snapshot_without_writes; groove::db::tests::subscribe_returns_current_rows_as_initial_message_then_future_deltas

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::subscribe
