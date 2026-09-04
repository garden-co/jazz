# INV-OK-14

- Status: now
- Coverage: untested

## Invariant

Base-table writes and durable index/view writes MUST be committed through one storage-atomic batch. A definitely-uncommitted final batch may roll back its runtime tick; a possibly-committed final batch MUST poison the `Database` instance and reject subsequent operations until a fresh reopen rebuilds from durable storage.

## Enforced by (tests)

NONE-FOUND

## Implementation

`src/db/commit.rs::Database::commit_pending_writes`; `src/storage/mod.rs::OrderedKvStorage::write_many_outcome`
