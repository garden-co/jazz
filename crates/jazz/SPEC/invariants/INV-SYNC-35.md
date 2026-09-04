# INV-SYNC-35

- Status: target
- Coverage: untested

## Invariant

A receiver MUST stage an initial/reset full closure or an exact authenticated predecessor-to-successor incremental manifest transition, then atomically and durably swap the active manifest/epoch, all facts, local IVM state/terminal, settlement frontier, and fast-known-state receipt before enqueuing any post-cut local publication. Crash recovery MUST expose either the old or new complete closure, never a partial closure, terminal, settlement, fast receipt, or local publication.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned manifest staging, authenticated transition verification, durable swap, crash recovery, and post-cut local publication
