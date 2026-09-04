# INV-LENS-6

- Status: now
- Coverage: ✓

## Invariant

Unknown-schema shape registrations MUST park and MUST register only after the named schema-version catalogue value arrives.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::shape_registration_parks_until_schema_version_catalogue_arrives`

## Implementation

`jazz/src/node/query_eval.rs::NodeState::register_shape`, `jazz/src/node/query_eval.rs::NodeState::drain_parked_shape_registrations`
