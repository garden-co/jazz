# INV-LOWER-11

- Status: now
- Coverage: untested

## Invariant

Prepared graph lowering MUST reject `!=` predicates against parameters until supported.

## Enforced by (tests)

NONE-FOUND

## Implementation

`jazz/src/node/query_eval.rs::apply_query_filters`
