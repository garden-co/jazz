# INV-HIST-13

- Status: now
- Coverage: untested

## Invariant

Re-ingesting the same commit unit with identical version rows in a different order MUST be idempotent and MUST NOT create a conflict.

## Enforced by (tests)

NONE-FOUND

## Implementation

`jazz/src/node/ingest.rs::ingest_known_transaction`
