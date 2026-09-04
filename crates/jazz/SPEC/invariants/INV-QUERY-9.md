# INV-QUERY-9

- Status: now
- Coverage: partial

## Invariant

Result-set material MUST include output rows plus matched include-reference and join/junction contribution rows, MUST exclude traversed non-matches and failed include paths from subscription payloads, and MUST apply read-policy/policy-atomic filtering before emission.

## Enforced by (tests)

`jazz::peer::tests::incremental_query_result_set_rebuilds_stale_closure_rows`; partial: include-reference, junction, non-match, and policy-atomic delivery cases remain uncovered

## Implementation

`node/views.rs::NodeState::expand_query_closure`; `node/views.rs::NodeState::query_output_closure_contribution`; `node/views.rs::NodeState::retain_policy_atomic_rows`
