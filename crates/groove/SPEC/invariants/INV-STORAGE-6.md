# INV-STORAGE-6

- Status: now
- Coverage: ✓

## Invariant

Table records MUST be stored as values in the table column family named by `TableSchema::name`, keyed by the encoded primary key derived from the row record.

## Enforced by (tests)

`groove::db::tests::commits_insert_update_and_delete_batches`; `groove::db::tests::composite_primary_keys_are_encoded_from_multiple_columns`

## Implementation

`db/mod.rs::Database::commit_batch`; `db/mod.rs::Database::commit_pending_writes`; `db/mod.rs::primary_key_bytes`; `storage/mod.rs::RecordStore::set`
