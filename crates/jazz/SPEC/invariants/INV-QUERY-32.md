# INV-QUERY-32

- Status: now
- Coverage: ✓

## Invariant

The payload layer carries SQL NULL as `Nullable(None)` and the public cell layer carries a caller-visible absent value; both internal layers MUST keep present-but-NULL distinguishable from an absent payload. The two collapse exactly once, at the public boundary, and nowhere earlier.

## Enforced by (tests)

`jazz::node::tests::harness::m3_maintained_one_shot_differential_oracle`; `aggregate_subscriptions::grouped_null_aggregate_membership_survives_absence_and_replacement`

## Implementation

`jazz/src/node/query_eval.rs::aggregate_payload_cell_value`; `jazz/src/node/query_eval.rs::current_row_from_aggregate_result_payload`
