# INV-STORAGE-5

- Status: prov
- Coverage: ✓

## Invariant

`ReopenableStorage::reopen` MUST preserve existing data while adding newly requested column families.

## Enforced by (tests)

`groove::storage::tests::memory_storage_reopen_adds_column_families_without_losing_data`

## Implementation

`storage/memory.rs::MemoryStorage::reopen`; `storage/rocksdb.rs::RocksDbStorage::reopen`
