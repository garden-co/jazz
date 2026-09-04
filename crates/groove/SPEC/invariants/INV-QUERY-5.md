# INV-QUERY-5

- Status: now
- Coverage: ✓

## Invariant

`MapProjectOp` MUST emit one output delta per input delta, copying only configured fields into the output descriptor and preserving the input weight.

## Enforced by (tests)

`groove::db::tests::project_subscriptions_emit_projected_records`

## Implementation

`groove/src/ivm/runtime/mod.rs::NodeState::update_map_project`
