# INV-STORAGE-2

- Status: now
- Coverage: ✓

## Invariant

A prefix `ScanRequest` MUST return exactly keys beginning with the supplied byte prefix in the requested lexicographic direction, including prefixes whose finite upper bound cannot be computed.

## Enforced by (tests)

`groove::storage::tests::prefix_returns_ordered_values_with_matching_prefix`; `groove::storage::tests::prefix_handles_prefixes_without_a_finite_upper_bound`; `groove::storage::tests::memory_storage_orders_scans_and_errors_on_missing_column_families`

## Implementation

`jazz-storage-rocksdb/src/lib.rs::RocksDbStorage::scan`; `jazz-storage-rocksdb/src/lib.rs::advance_prefix_upper_bound`; `storage/memory.rs::MemoryStorage::scan`
