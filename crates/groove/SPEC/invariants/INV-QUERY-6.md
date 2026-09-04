# INV-QUERY-6

- Status: now
- Coverage: ✓

## Invariant

`UnwrapNullableOp` MUST drop `Nullable(None)` input deltas, unwrap `Nullable(Some(_))` to the inner value, and preserve the original delta weight.

## Enforced by (tests)

`groove::db::tests::unwrap_nullable_graph_drops_none_and_unwraps_present_values`; `groove::db::tests::unwrap_nullable_retractions_flow_symmetrically`

## Implementation

`groove/src/ivm/runtime/mod.rs::NodeState::update_unwrap_nullable`
