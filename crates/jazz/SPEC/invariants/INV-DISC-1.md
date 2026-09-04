# INV-DISC-1

- Status: prov
- Coverage: untested

## Invariant

node-core work must remain simulation-first. Load-bearing rule: production node semantics are exercised through deterministic inputs and explicit method/event surfaces; no transport, thread, randomness, or clock dependency should be hidden inside node logic. This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
