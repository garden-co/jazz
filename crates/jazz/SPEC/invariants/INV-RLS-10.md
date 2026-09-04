# INV-RLS-10

- Status: now
- Coverage: ✓

## Invariant

Query-driven sync MUST compose the root table read policy into the subscribed query and bind policy claims from server-authenticated identity so a client cannot widen visibility by supplying claim params.

## Enforced by (tests)

jazz::node::tests::policies_rls::composed_read_policy_grants_and_revokes_incrementally

## Implementation

jazz/src/node/query_eval.rs::NodeState::policy_composed_shape_binding
