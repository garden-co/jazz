# INV-STORAGE-16

- Status: now
- Coverage: ✓

## Invariant

Inserts MUST reject an existing primary key, including keys introduced by earlier operations in the same `DatabaseBatch`.

## Enforced by (tests)

`groove::db::tests::inserts_over_existing_primary_keys_are_rejected`; `groove::db::tests::inserts_over_primary_keys_created_earlier_in_the_same_batch_are_rejected`

## Implementation

`db/mod.rs::compute_table_deltas`
