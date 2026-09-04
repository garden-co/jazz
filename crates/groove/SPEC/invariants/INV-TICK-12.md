# INV-TICK-12

- Status: now
- Coverage: ✓

## Invariant

Snapshot and shape hydration MUST rebuild arrangements with `ArrangementUpdateMode::Replace` rather than accumulating a snapshot over existing arrangement contents.

## Enforced by (tests)

groove::tests::snapshot_subscription_regressions::new_subscriber_uses_current_state_not_stale_hydrated_accumulated

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::hydration_snapshot; groove/src/ivm/runtime/mod.rs::IvmRuntime::hydrate_shape_graph; groove/src/ivm/runtime/join.rs::ArrangementState::apply_update
