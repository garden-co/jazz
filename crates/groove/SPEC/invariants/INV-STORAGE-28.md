# INV-STORAGE-28

- Status: now
- Coverage: ✓

## Invariant

`put_if_absent` and `compare_and_delete` MUST be atomic at one persistence scope: a backend serializes them across concurrent handles or enforces exclusive open; comparison uses exact stored bytes so stale cleanup cannot delete a reinstalled logical value (ABA).

## Enforced by (tests)

`groove::storage::tests::memory_storage_conditionals_are_atomic_and_aba_safe`; `groove::storage::idb::tests::independent_handles_preserve_one_conditional_winner`; `groove::chunks::tests::stale_chunk_delete_cannot_remove_a_newer_durable_mapping`; `plain_row_receipt::fresh_open_requires_dropping_the_original_exclusive_handle`; `ordered_kv::conditional_mutations_are_atomic_across_handles_and_aba_safe`

## Implementation

`groove/src/storage/{memory,idb}.rs`; `jazz-storage-{rocksdb,sqlite}/src/lib.rs`; `groove/src/chunks.rs::OrderedChunkStorage`
