# INV-STORAGE-7

- Status: now
- Coverage: ✓

## Invariant

Public insert/update values MUST be interpreted in `TableSchema.columns` declaration order, independent of the `RecordDescriptor` physical encoding order.

## Enforced by (tests)

`groove::db::tests::inserts_accept_values_in_table_declaration_order_even_when_storage_order_differs`; `groove::schema::tests::table_schema_maps_columns_to_record_schema`

## Implementation

`db/mod.rs::encode_record`; `schema.rs::TableSchema::record_schema`; `records/mod.rs::RecordDescriptor::from_logical_fields`
