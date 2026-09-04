# INV-SHAPE-1

- Status: now
- Coverage: ✓

## Invariant

Graphs containing `BindingSource` MUST NOT be evaluated through ordinary `query_snapshot`, `subscribe`, or `subscribe_query`; they MUST be evaluated only through prepared-shape APIs.

## Enforced by (tests)

groove::db::tests::binding_sources_are_rejected_outside_prepared_shapes; groove::db::tests::prepared_subscription_lowers_parameter_predicates_to_shape_subscriptions

## Implementation

groove/src/ivm/runtime/mod.rs::IvmRuntime::query_snapshot; groove/src/ivm/runtime/mod.rs::IvmRuntime::subscribe; groove/src/ivm/runtime/mod.rs::builder_contains_binding_source; groove/src/ivm/planner.rs::plan_query
