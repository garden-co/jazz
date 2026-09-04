# INV-STORAGE-31

- Status: now
- Coverage: ✓

## Invariant

A durable adapter MUST validate its epoch-pinned physical manifest before mutating a pre-existing store; backend commit/WAL sync is the durability boundary, while flushes/checkpoints are maintenance; engine files are not logical interchange. The caller selects one closed, sorted opaque codec profile; omission, addition, duplication, or substitution fails admission, while adapters do not interpret higher-layer codec semantics. IndexedDB's fixed `storage-manifest`/`epoch` record pins its epoch, adapter/page format, selected codec profile, page size, and checksum parameters; its separate `replica-node-v1` record is an exact 16-byte physical-replica identity, created atomically with a fresh manifest and validated before opening Jazz.

## Enforced by (tests)

`ordered_kv::{physical_header_ddl_and_jazz_blobs_are_pinned_across_reopen,rejects_wrong_sqlite_header_before_changing_foreign_store}`; `plain_row_receipt::durable_fresh_open_preserves_ordered_data_and_rejects_partial_batches`; `packages/jazz-tools/tests/browser/indexeddb-physical-epoch.test.ts`; `indexeddb-page-store::{persists_one_random_node_per_physical_replica_not_per_logical_database_name,rejects_a_missing_or_malformed_physical_replica_node_before_touching_pages}`

## Implementation

`groove/src/storage/{manifest,mod}.rs`; `jazz-storage-{sqlite,rocksdb}/src/lib.rs`; `jazz/src/storage_codec_profile.rs`; `jazz-tools/src/runtime/indexeddb-page-store.ts`
