# INV-STORAGE-15

- Status: now
- Coverage: ✓

## Invariant

Table writes MUST reject rows whose primary-key values do not match the declared `PrimaryKeyColumn.key_type`, and MUST reject table writes for tables with no primary key.

## Enforced by (tests)

`groove::db::tests::rejects_primary_key_type_mismatches_before_writing`; `groove::db::tests::rejects_tables_without_primary_keys`

## Implementation

`db/mod.rs::primary_key_bytes`; `db/mod.rs::ensure_primary_key_value_type`
