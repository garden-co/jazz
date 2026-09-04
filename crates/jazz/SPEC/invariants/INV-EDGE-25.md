# INV-EDGE-25

- Status: target
- Coverage: untested

## Invariant

A fate authority receiving a commit (mergeable or exclusive) through a scope-isolated client relay MUST authorize it only under that relay attachment's immutable server-admitted foreground binding, then pass an internal non-wire proof into terminal admission. Transaction authorship, relay transport, `SYSTEM`, persisted state, stale epochs, and mutable session-claim refresh MUST NOT substitute for or widen that binding.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned scope-relay admission capability and terminal authorization proof
