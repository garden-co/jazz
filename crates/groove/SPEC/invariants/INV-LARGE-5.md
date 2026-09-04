# INV-LARGE-5

- Status: now
- Coverage: ✓

## Invariant

Each operator MUST request only the minimum conservative evidence needed for exact inline-equivalent semantics.

## Enforced by (tests)

`large_value_query::count_star_does_not_fetch_an_unused_indirect_column`; `large_value_query::projection_does_not_fetch_an_unselected_indirect_column`; `large_value_query::filter_does_not_fetch_an_indirect_column_the_predicate_does_not_reference`; `large_value_query::join_fetches_only_key_and_selected_large_fields`; `large_value_query::lexical_predicate_stops_chunk_requests_after_decisive_prefix_mismatch`; `large_value_query::root_array_json_pointer_stops_after_the_selected_complete_element`

## Implementation

`groove/src/ivm/runtime/evaluator.rs`; `groove/src/large_values.rs`
