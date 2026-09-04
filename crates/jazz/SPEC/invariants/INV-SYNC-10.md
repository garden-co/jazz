# INV-SYNC-10

- Status: now
- Coverage: ✓

## Invariant

A reset-result-set `ViewUpdate` MUST set `reset_result_set = true`; applying it MUST clear the receiver's settled subscription result set before applying the replacement result members and program facts.

## Enforced by (tests)

`jazz::peer::tests::rehydrate_keeps_peer_payload_dedup_but_resends_result_set`; `jazz::node::tests::sync::cold_reset_bulk_ingest_matches_incremental_ingest`

## Implementation

`peer.rs::rehydrate_query`; `peer.rs::view_update_reset_result_set`; `node/views.rs::apply_view_update`
