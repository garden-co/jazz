# INV-QUERY-31

- Status: now
- Coverage: ✓

## Invariant

Aggregate output types are fixed by function: `count` is `U64` and never null; `sum`, `min`, `max` are nullable over the non-nullable base type of their input; `avg` is `Nullable(F64)`. `sum` MUST NOT silently widen, and an overflow MUST fail with a named error rather than wrapping, saturating, or promoting.

## Enforced by (tests)

`aggregate_sql_semantics::non_count_aggregate_outputs_are_always_nullable`; `aggregate_sql_semantics::all_null_inputs_return_null_except_for_counts`; `aggregate_sql_semantics::sum_overflow_fails_with_a_named_error_at_the_declared_width`

## Implementation

`groove/src/ivm/runtime/mod.rs::aggregate_output_type`; `groove/src/ivm/runtime/mod.rs::aggregate_sum`; `jazz/src/node/query_eval.rs::aggregate_result_column_type`
