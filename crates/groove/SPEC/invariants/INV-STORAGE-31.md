# INV-STORAGE-31

- Status: now
- Coverage: ✓

## Invariant

A durable adapter MUST validate its epoch-pinned physical manifest before mutating a pre-existing store; backend commit/WAL sync is the durability boundary, while flushes/checkpoints are maintenance; engine files are not logical interchange.

## Enforced by (tests)

`ordered_kv::{physical_header_ddl_and_jazz_blobs_are_pinned_across_reopen,rejects_wrong_sqlite_header_before_changing_foreign_store}`; `plain_row_receipt::durable_fresh_open_preserves_ordered_data_and_rejects_partial_batches`

## Implementation

`jazz-storage-{sqlite,rocksdb}/src/lib.rs`
