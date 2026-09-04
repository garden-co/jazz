# INV-SHAPE-3

- Status: now
- Coverage: ✓

## Invariant

A prepared-query internal graph output MUST include every binding key column needed for routing, while `PreparedShape::output` and bound subscription rows MUST expose only the public query projection; projected output names that collide with parameter names MUST be rejected except for the same source field.

## Enforced by (tests)

groove::db::tests::prepared_subscription_lowers_parameter_predicates_to_shape_subscriptions

## Implementation

groove/src/ivm/planner.rs::append_missing_binding_fields; groove/src/ivm/planner.rs::plan_prepared_shape; groove/src/ivm/runtime/mod.rs::ShapeNotificationProjection
