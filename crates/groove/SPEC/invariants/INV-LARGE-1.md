# INV-LARGE-1

- Status: now
- Coverage: ✓

## Invariant

Inline and indirect physical representations MUST be unobservable to every logical scalar consumer.

## Enforced by (tests)

`large_value_query::indirect_string_materializes_as_the_ordinary_logical_query_value`; `large_value_query::indirect_scalars_materialize_inside_composite_values`; `large_value_query::predicates_compare_indirect_strings_by_logical_value`; `large_value_query::predicates_compare_present_nullable_indirect_strings_logically`; `large_value_query::subscription_materializes_large_insert_and_update_deltas_atomically`

## Implementation

`groove/src/ivm/runtime/evaluator.rs::materialize_indirect_fields`; `groove/src/records/tests.rs::indirect_string_uses_the_same_logical_value_type_with_an_explicit_physical_arm`
