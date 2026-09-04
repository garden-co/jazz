# INV-STORAGE-4

- Status: now
- Coverage: ✓

## Invariant

`write_many` MUST apply all `Set`/`Delete` operations atomically at the storage-operation level, and a missing column family in the operation list MUST leave earlier valid operations unapplied.

## Enforced by (tests)

`groove::storage::tests::write_many_writes_all_operations_atomically`; `groove::storage::tests::write_many_fails_without_writing_when_column_family_is_missing`; `groove::storage::tests::write_many_can_mix_sets_and_deletes_atomically`; `groove::storage::tests::memory_storage_write_many_validates_column_families_before_writing`; `groove::db::tests::invalid_batches_do_not_partially_write_valid_earlier_operations`

## Implementation

`storage/rocksdb.rs::RocksDbStorage::write_many`; `storage/memory.rs::MemoryStorage::write_many`; `db/mod.rs::Database::commit_pending_writes`
