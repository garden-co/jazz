# INV-TICK-14

- Status: now
- Coverage: ✓

## Invariant

Prepared-shape output routing MUST update per-binding materialized weights and MUST send each output delta only to active subscriptions whose `BindingKey` equals the projected output key.

## Enforced by (tests)

groove::db::tests::prepared_binding_join_hydrates_anti_join_input; groove::db::tests::prepared_binding_join_hydrates_filtered_unwrapped_anti_join_input

## Implementation

groove/src/ivm/runtime/mod.rs::route_shape_records
