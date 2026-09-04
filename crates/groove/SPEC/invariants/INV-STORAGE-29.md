# INV-STORAGE-29

- Status: now
- Coverage: ✓

## Invariant

An explicit ordered scan request with a finite item bound MUST yield at most that many entries in its requested order, and backends MUST stop traversal/hydration at the bound rather than treating it as caller-side collection advice.

## Enforced by (tests)

`groove::storage::tests::explicit_scan_request_preserves_bounds_direction_and_hard_limit`; `groove::storage::tests::bounded_transaction_scan_stops_base_cursor_after_logical_output_is_full`

## Implementation

`groove/src/storage/mod.rs::ScanRequest`; `groove/src/storage/memory.rs::MemoryStorage::scan`; `idb-tree/src/lib.rs::IdbTree::range_limit`; `jazz-storage-rocksdb/src/lib.rs::RocksDbCursor`; `groove/src/storage/mod.rs::StagedWriteOverlay::scan`
