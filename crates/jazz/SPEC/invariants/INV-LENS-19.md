# INV-LENS-19

- Status: now
- Coverage: ✓

## Invariant

Policy evaluation under lenses MUST translate data into the pinned permission evaluation schema and MUST NOT translate policy bundles.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::rls_policy_under_lenses_evaluates_translated_data_against_pinned_policy`

## Implementation

`jazz/src/node/policy.rs::NodeState::policy_projection_for_version_row`; `jazz/src/node/policy.rs::NodeState::policy_projection_for_version_record`; `jazz/src/node/policy.rs::NodeState::write_policy_allows_version_record`; `jazz/src/node/policy.rs::NodeState::read_policy_allows_version`
