# INV-QUERY-4

- Status: now
- Coverage: untested

## Invariant

Shape registration MUST reject an AST whose content-addressed id does not match `shape_id`, and MUST park registrations naming an unknown schema version until the schema catalogue arrives.

## Enforced by (tests)

NONE-FOUND

## Implementation

`node/query_eval.rs::NodeState::register_shape`; `node/query_eval.rs::NodeState::drain_parked_shape_registrations`
