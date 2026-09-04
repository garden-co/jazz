# INV-STORAGE-35

- Status: now
- Coverage: ✓

## Invariant

Jazz's epoch-1 class-CF view MUST admit only marker key `groove-storage-layout` with value `class-cf-v1`, use the specified ordered logical-family classifier and physical class names, and frame mapped keys as the u32 big-endian UTF-8 logical-family length followed by the logical-family UTF-8 bytes then the logical key. Missing, malformed, old, or future markers in a nonempty mapped/class store fail closed before logical access; no legacy migration or fallback exists within epoch 1.

## Enforced by (tests)

`groove::storage::tests::{class_layout_marker_and_mapped_key_receipt_is_exact,class_layout_rejects_unknown_old_future_and_malformed_markers_before_logical_access}`; `ordered_kv::{class_layout_v1_writes_exact_sqlite_marker_and_mapped_key_receipt,class_layout_v1_reopen_rejects_a_future_marker_without_normalizing_it}`; `jazz_storage_rocksdb::tests::class_layout_v1_writes_exact_rocks_marker_and_mapped_key_receipt`

## Implementation

`groove/src/storage/mod.rs::{StorageLayout,jazz_physical_class,LayoutStorage}`; `jazz-storage-{sqlite,rocksdb}` physical receipts
