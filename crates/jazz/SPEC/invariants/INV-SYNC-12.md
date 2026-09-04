# INV-SYNC-12

- Status: now
- Coverage: ✓

## Invariant

Downstream subscription view updates MUST contain accepted/settled state only and MUST NOT emit pending versions to non-origin peers.

## Enforced by (tests)

`jazz::node::tests::sync::m2_writer_core_reader_converges_against_oracle`; `jazz::tests::four_tier::four_tier_topology_relays_pending_units_and_core_fates`

## Implementation

`node/views.rs::view_update_for_query_binding_with_peer_payload_inventory_and_plan`; `node/query_eval.rs::query_rows`; `node/global_state.rs::global_current_updates`
