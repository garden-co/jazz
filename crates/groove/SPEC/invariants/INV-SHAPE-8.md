# INV-SHAPE-8

- Status: now
- Coverage: ✓

## Invariant

Shape deltas MUST be routed by projecting the prepared output record through `output_key_fields`, or the explicit routing graph output through `routing_key_fields`, into the binding descriptor, and MUST be sent only to subscribers registered for that binding key.

## Enforced by (tests)

`groove::db::tests::parameterized_shape_hydrates_and_routes_by_param`; `groove::db::tests::prepared_subscription_uses_route_terminal_with_clean_public_projection`

## Implementation

groove/src/ivm/runtime/mod.rs::route_shape_records; groove/src/ivm/runtime/mod.rs::IvmRuntime::prepare_one_sink_with_routing
