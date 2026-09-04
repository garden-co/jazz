# INV-SYNC-17

- Status: now
- Coverage: ✓

## Invariant

`ViewUpdate` emission for a result add MUST include enough deletion-register context to reconstruct visible absence/presence for that row.

## Enforced by (tests)

`jazz::peer::tests::whole_table_incremental_delta_ships_restore_register_witness`; `jazz::peer::tests::rehydrate_keeps_peer_payload_dedup_but_resends_result_set`

## Implementation

`node/views.rs::view_update_for_query_binding_with_peer_payload_inventory_and_plan`; `node/views.rs::view_update_for_query_result_delta`
