# INV-QUERY-20

- Status: now
- Coverage: ✓

## Invariant

Query payload dedup MUST be per peer across all subscriptions for complete transaction payloads: already-covered complete payloads are referenced via `peer_payload_inventory.complete_tx_payloads`, and partial bundles, including partial mergeable or exclusive bundles, MUST NOT establish complete-transaction payload coverage.

## Enforced by (tests)

`jazz::node::tests::harness::query_payload_dedup_is_per_peer_across_subscriptions`; `jazz::node::tests::harness::partial_mergeable_payload_does_not_establish_tx_level_complete_tx_ref`; `jazz::node::tests::harness::partial_exclusive_payload_does_not_establish_tx_level_complete_tx_ref`

## Implementation

`node/views.rs::NodeState::view_update_for_query_binding_with_peer_payload_inventory_and_plan`; `peer.rs::PeerState::record_outgoing_view_update`
