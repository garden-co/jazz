# INV-SYNC-13

- Status: now
- Coverage: ✓

## Invariant

Downstream view construction MUST apply the peer identity's read policy before emitting result-set entries, version bundles, complete tx payload refs.

## Enforced by (tests)

`jazz::node::tests::policies_rls::owner_only_read_narrows_view_updates_per_peer_identity`; `jazz::node::tests::policies_rls::composed_read_policy_grants_and_revokes_incrementally`; `jazz::tests::four_tier::edge_peer_terminates_client_identity_and_relays_upstream`

## Implementation

`peer.rs::PeerRole::identity`; `node/views.rs::view_update_for_query_binding_with_peer_payload_inventory_and_plan`
