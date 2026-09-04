# INV-STORAGE-24

- Status: now
- Coverage: ✓

## Invariant

Persisted index scans MUST decode the persisted index record's `"value"` as primary-key bytes and fetch the current base table record; if the base record is missing for a primary-key table, the index MUST be treated as invalid.

## Enforced by (tests)

`groove::db::tests::persisted_index_scan_treats_missing_primary_key_record_as_invalid`

## Implementation

`db/mod.rs::Database::decode_raw_index_entries`
