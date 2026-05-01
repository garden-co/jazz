# Verbose batch payloads

## What

Replayable settlements still repeat per-member batch identity that is already fixed by the outer batch, wasting durable bytes and in-memory copies.

## Priority

high

## Notes

- Where:
  - `crates/jazz-tools/src/batch_fate.rs`
  - `crates/jazz-tools/src/runtime_core/writes.rs`
  - `crates/jazz-tools/src/sync_manager/inbox.rs`
- `SealedBatchMember` is now object-plus-digest only.
- `BatchSettlement` successful cases already know the outer `batch_id`, but each `VisibleBatchMember` repeats it.
- Direction: shrink settlement members to the minimum identity the batch model actually needs, likely moving more batch-level context to the outer settlement.
