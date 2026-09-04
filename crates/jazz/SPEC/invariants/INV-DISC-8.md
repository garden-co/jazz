# INV-DISC-8

- Status: prov
- Coverage: untested

## Invariant

out-of-order, duplicate, restart, and rehydrate hazards must be first-class seeded-test actions. The M3 harness duplicates upstream/fate/view messages, delivers child before parent, restarts readers/core, emits rehydrates, and asserts quiescent drains (support.rs lines 1200-1600, 1637-1688). This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
