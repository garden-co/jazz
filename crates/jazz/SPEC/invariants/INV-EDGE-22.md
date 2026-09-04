# INV-EDGE-22

- Status: target
- Coverage: untested

## Invariant

Authoritative result state retained by a relay MUST be identified by the complete canonical query identity and exact immutable policy binding; result members, generations, receipts, repair state, and persistence from different policy bindings MUST never share a mutable result identity.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned policy-scoped authority-result identity and durable settled-view state
