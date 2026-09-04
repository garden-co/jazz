# INV-QUERY-33

- Status: now
- Coverage: ✓

## Invariant

A group present with a NULL aggregate, an absent group, and a group whose aggregate value changed are three distinct outcomes the delivered result MUST distinguish. An empty group and an all-NULL group are both present-with-NULL, not absent.

## Enforced by (tests)

`aggregate_subscriptions::grouped_null_aggregate_membership_survives_absence_and_replacement`

## Implementation

`jazz/src/node/maintained_subscription_view.rs::apply_aggregate_result_delta`; `jazz/src/node/query_eval.rs::current_row_from_aggregate_result_payload`
