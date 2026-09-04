# INV-SYNC-8

- Status: now
- Coverage: ✓

## Invariant

A view server MUST use `peer_payload_inventory.complete_tx_payloads` only for tx-level complete payloads covered by the peer payload inventory; payload dedup MUST be peer-scoped, not subscription-scoped, and partial bundles MUST remain eligible for later payload emission until complete-tx payload coverage is established.

## Enforced by (tests)

`jazz::peer::tests::peer_state_dedups_version_payloads_across_subscription_views`

## Implementation

`peer.rs::PeerState::shipped_complete_tx_payloads`; `peer.rs::record_outgoing_view_update`; `node/views.rs::view_update_for_query_binding_with_peer_payload_inventory_and_plan`; `node/views.rs::view_update_for_query_result_delta`
