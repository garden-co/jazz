# INV-EDGE-17

- Status: now
- Coverage: ✓

## Invariant

An edge permission-scope subscription MUST be keyed by `(policy_shape, writer_claim)` — the write policy's query shape bound to the writer's `claim("sub")` — and MUST NOT hydrate a whole-table scope. A public-write table (no write policy) opens no scope and settles immediately.

## Enforced by (tests)

`jazz::tests::four_tier::edge_permission_scope_is_write_policy_claim_not_whole_table`; `jazz::tests::four_tier::edge_permission_scopes_are_keyed_by_policy_shape_and_writer_claim`

## Implementation

`jazz/src/node/query_eval.rs::NodeState::permission_scope_shape_binding`; `jazz/src/peer.rs::PeerState::unsettled_permission_scope_subscriptions`
