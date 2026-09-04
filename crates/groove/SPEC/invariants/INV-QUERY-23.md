# INV-QUERY-23

- Status: now
- Coverage: ✓

## Invariant

TopBy MUST order each partition's positive-multiplicity records by order_cols with declared directions, then tie_cols ascending, then encoded full-record bytes ascending; the total order MUST NOT depend on arrival or iteration order.

## Enforced by (tests)

`groove::db::tests::top_by_replaces_window_tie_with_distinct_record_on_delete`; `groove::db::tests::top_by_uses_stable_tie_field`

## Implementation

`groove/src/ivm/runtime/mod.rs::top_by_window_from_records`
