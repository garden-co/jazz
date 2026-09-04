# INV-SYNC-31

- Status: target
- Coverage: untested

## Invariant

A downstream subscription MUST synchronize canonical authored facts with their logical table, row, transaction, authored schema, branch/source, fate, and witness identity intact under a closure manifest whose epoch, per-class inventory, and digest prove exact completeness; a projected application/terminal row is never replicated truth.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned canonical witness-closure sync ingress and manifest validation
