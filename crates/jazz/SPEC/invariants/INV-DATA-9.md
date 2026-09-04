# INV-DATA-9

- Status: now
- Coverage: ✓

## Invariant

A declared `MergeStrategy::Counter` MUST be accepted only on non-nullable integer columns. Public `Integer` and `BigInt` lower to `I32` and `I64`; internal schemas may use `U8`, `U16`, `U32`, `U64`, `I32`, or `I64`.

## Enforced by (tests)

`jazz::schema::tests::counter_merge_strategy_rejects_string_columns`; `jazz::schema::tests::counter_merge_strategy_rejects_nullable_integer_columns`; `jazz::tools::public_schema_convert::tests::converts_counter_merge_strategy_on_integer_columns`; `jazz::tools::public_schema_convert::tests::converts_counter_merge_strategy_on_bigint_columns`; `jazz::tools::public_schema_convert::tests::rejects_counter_merge_strategy_on_nullable_integer_columns_with_exact_path`; `jazz::tools::public_schema_convert::tests::rejects_counter_merge_strategy_on_non_integer_columns_with_exact_path`

## Implementation

`jazz/src/schema.rs::RuntimeSchema::validated`; `jazz/src/schema.rs::is_counter_column_type`; `jazz/src/tools/public_api/types/schema.rs::ColumnDescriptor::validate_merge_strategy`; `jazz/src/tools/public_schema_convert.rs::convert_merge_strategy`
