# INV-BENCH-8

- Status: prov
- Coverage: untested

## Invariant

S5 must model append streams as full content: bytes rewrites in streamDocs, not a userland event log, and compare against fsync append-log, SQLite WAL, and zstd anchors. This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
