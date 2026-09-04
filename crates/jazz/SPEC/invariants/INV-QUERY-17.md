# INV-QUERY-17

- Status: now
- Coverage: ✓

## Invariant

When a row remains in a query result but its visible content version changes, result-set entries MUST track the new `TxId` even if projected cell values are identical.

## Enforced by (tests)

`jazz::peer::tests::incremental_query_result_set_tracks_identical_cell_rewrite_tx_id`

## Implementation

`node/views.rs::NodeState::query_output_entry_from_delta`; `peer.rs::PeerState::query_update_from_deltas`
