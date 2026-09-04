# INV-STORAGE-20

- Status: now
- Coverage: ✓

## Invariant

Directly exposed record stores MUST be typed record stores with record-encoded values and order-preserving typed primary keys, while bypassing table batches, primary-key table scans, durable index maintenance, query planning, and IVM ticks. A single trailing variable-width `Bytes` value column MUST encode as exactly the stored bytes.

## Enforced by (tests)

`groove::db::tests::direct_record_store_stores_ordered_records_independent_of_tables`; `groove::db::tests::direct_record_store_tuple_keys_set_prefix_order_and_reopen_symmetrically`

## Implementation

`schema.rs::DirectRecordStoreSchema`; `db/facade.rs::Database::direct_record_store`; `db/storage_helpers.rs::DirectRecordStore`
