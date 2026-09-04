# INV-STORAGE-19

- Status: now
- Coverage: ✓

## Invariant

Runtime storage reads during a staged tick MUST observe staged set/delete operations before committed storage, including same-tick durable `Persist` writes.

## Enforced by (tests)

`groove::storage::tests::staged_overlay_reads_staged_sets_and_deletes_before_base_storage`

## Implementation

`storage/mod.rs::StagedWriteOverlay`; `ivm/runtime/mod.rs::IvmRuntime::tick_staged`
