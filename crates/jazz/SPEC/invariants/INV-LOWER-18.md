# INV-LOWER-18

- Status: now
- Coverage: ✓

## Invariant

Counter merge strategy MUST NOT be accepted for nullable or non-integer columns.

## Enforced by (tests)

`jazz::schema::tests::counter_merge_strategy_rejects_string_columns`; `jazz::schema::tests::counter_merge_strategy_rejects_nullable_integer_columns`; `jazz::tools::public_schema_convert::tests::rejects_counter_merge_strategy_on_nullable_integer_columns_with_exact_path`; `jazz::tools::public_schema_convert::tests::rejects_counter_merge_strategy_on_non_integer_columns_with_exact_path`

## Implementation

`jazz/src/schema.rs::RuntimeSchema::validated`; `jazz/src/schema.rs::is_counter_column_type`; `jazz/src/tools/public_api/types/schema.rs::ColumnDescriptor::validate_merge_strategy`; `jazz/src/tools/public_schema_convert.rs::convert_merge_strategy`
