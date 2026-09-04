# INV-SYNC-11

- Status: now
- Coverage: ✓

## Invariant

Reset-result-set `ViewUpdate`s MUST preserve per-peer payload dedup when peer state survives, while resending the subscription result set as a complete replacement.

## Enforced by (tests)

`jazz::peer::tests::rehydrate_keeps_peer_payload_dedup_but_resends_result_set`; `jazz::peer::tests::peer_state_records_current_result_set_and_can_rehydrate`

## Implementation

`peer.rs::rehydrate_query`; `peer.rs::merge_rehydrate_diff`; `peer.rs::record_outgoing_view_update_metadata`
