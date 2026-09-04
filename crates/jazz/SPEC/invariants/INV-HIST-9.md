# INV-HIST-9

- Status: now
- Coverage: ✓

## Invariant

`MergeStrategy::Counter` MUST be declared only on non-nullable integer user columns.

## Enforced by (tests)

`jazz::schema::tests::counter_merge_strategy_rejects_string_columns`; `jazz::schema::tests::counter_merge_strategy_rejects_nullable_integer_columns`; `jazz::tools::public_schema_convert::tests::rejects_counter_merge_strategy_on_nullable_integer_columns_with_exact_path`; `jazz::tools::public_schema_convert::tests::rejects_counter_merge_strategy_on_non_integer_columns_with_exact_path`

## Implementation

`jazz/src/schema.rs::RuntimeSchema::validated`; `jazz/src/tools/public_api/types/schema.rs::ColumnDescriptor::validate_merge_strategy`; `jazz/src/tools/public_schema_convert.rs::convert_merge_strategy`
