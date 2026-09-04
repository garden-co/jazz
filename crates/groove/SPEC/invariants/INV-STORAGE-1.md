# INV-STORAGE-1

- Status: now
- Coverage: ✓

## Invariant

`OrderedKvStorage::scan(ScanRequest)` MUST return range results in the requested lexicographic direction and include keys `>= start` while excluding keys `>= end`.

## Enforced by (tests)

`groove::storage::tests::range_returns_ordered_values_between_start_and_end`; `groove::storage::tests::scans_visit_ordered_values_without_materializing_in_storage_api`; `groove::storage::tests::memory_storage_orders_scans_and_errors_on_missing_column_families`

## Implementation

`jazz-storage-rocksdb/src/lib.rs::RocksDbStorage::scan`; `storage/memory.rs::MemoryStorage::scan`
