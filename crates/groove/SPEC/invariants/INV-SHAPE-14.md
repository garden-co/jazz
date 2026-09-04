# INV-SHAPE-14

- Status: now
- Coverage: ✓

## Invariant

`Database::bind` MUST accept exactly one value for each prepared parameter name, MUST reject missing/duplicate/unknown names, and MUST pass values to `bind_shape` in prepared parameter order.

## Enforced by (tests)

groove::db::tests::prepared_subscription_validates_named_bindings

## Implementation

groove/src/db/mod.rs::Database::bind
