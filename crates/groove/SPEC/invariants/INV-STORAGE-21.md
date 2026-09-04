# INV-STORAGE-21

- Status: now
- Coverage: ✓

## Invariant

`DatabaseSchema::column_families()` MUST include the `"indices"` column family whenever any table declares an `IndexSchema`, and MUST omit it when no schema index exists.

## Enforced by (tests)

`groove::schema::tests::column_families_include_indices_family_when_any_table_declares_index`

## Implementation

`schema.rs::DatabaseSchema::column_families`
