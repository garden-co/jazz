# INV-EDGE-24

- Status: target
- Coverage: untested

## Invariant

Application and wire callers MUST NOT choose internal authority-result source identities or policy bindings. The receiving topology assigns and validates them from the authenticated connection and admitted session.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned inbound normalization and topology-owned authority-result construction
