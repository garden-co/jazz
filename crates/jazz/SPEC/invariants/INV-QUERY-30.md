# INV-QUERY-30

- Status: now
- Coverage: ✓

## Invariant

An aggregate result member's identity MUST be derived structurally from its group key, and a scalar global aggregate MUST lower to one fixed synthetic identity. Neither the identity nor any delivery decision keyed on it may be derived from, or matched against, a constructed name such as `<table>_aggregate`; filtering delivery by string comparison against a table name is forbidden.

## Enforced by (tests)

`aggregate_subscriptions::aggregate_subscription_count_and_grouped_sum_track_full_state`

## Implementation

`jazz/src/node/query_engine/lowering.rs::aggregate_result_membership_fields`; `jazz/src/node/query_eval.rs::is_public_aggregate_result_member`
