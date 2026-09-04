# INV-DISC-10

- Status: prov
- Coverage: untested

## Invariant

crash/restart recovery must rebuild in-memory node discipline state from storage, not from transport/session state. recover_from_storage rebuilds aliases, schema aliases, branch metadata, HLC/global timestamp state, pending edges, and rejected transaction headers (recovery.rs lines 48-180); M3 harness restarts reader/core nodes from storage (support.rs lines 1551-1590). This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
