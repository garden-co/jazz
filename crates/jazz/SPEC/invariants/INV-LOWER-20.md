# INV-LOWER-20

- Status: now
- Coverage: ✓

## Invariant

RLS policy declarations MUST be valid Jazz query shapes; read policy and write admission MUST lower through the query engine. Write admission MUST evaluate identity-aware old/candidate rows against the policy-pinned schema, and inherited write operations MUST select the matching parent write clause.

## Enforced by (tests)

`jazz::schema::tests::read_policy_validates_against_complete_schema`; `jazz::node::tests::harness::lowered_write_policy_operation_matrix`; `jazz::node::tests::harness::lowered_write_policy_covers_deep_inherited_write_chains`; `jazz::node::tests::harness::lowered_write_policy_keeps_v1_policy_pinned_after_table_rename`

## Implementation

`jazz/src/node/query_eval.rs::NodeState::write_policy_query_allows_candidate`; `jazz/src/node/query_eval.rs::NodeState::branch_write_policy_query_allows_candidate`; `jazz/src/node/policy.rs::NodeState::write_policy_allows_version_record`
