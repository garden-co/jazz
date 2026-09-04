# INV-RLS-5

- Status: now
- Coverage: ✓

## Invariant

Downstream view emission for a non-system peer MUST only add result members, program facts, and version bundles whose relevant content/deletion versions pass that peer identity's read policy.

## Enforced by (tests)

jazz::node::tests::policies_rls::owner_only_read_narrows_view_updates_per_peer_identity; jazz::node::tests::policies_rls::join_policy_authorizes_writes_reads_and_next_emission_revocation

## Implementation

jazz/src/node/views.rs::NodeState::view_update_for_query; jazz/src/node/views.rs::NodeState::version_bundle_for_view; jazz/src/node/policy.rs::NodeState::result_set_entry_read_policy_allows
