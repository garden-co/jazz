# INV-QUERY-8

- Status: now
- Coverage: partial: active builders still mostly emit ordinary current-content row members

## Invariant

Query `ViewUpdate` result sets MUST be addressed by a canonical program instance and carry typed result membership with enough version/read-view context to distinguish content versions, deletion-register visibility, branch/historic membership, synthetic rows, and path tuples. Real-row members MUST expose the ordinary current-row `(table, row_uuid, content_tx_id)` projection only as a compatibility/payload-bundling projection, not as the complete identity.

## Enforced by (tests)

`jazz::node::tests::view_update_result_set_matches_groove_current_rows_for_seeded_commits`; `jazz::peer::tests::incremental_query_result_set_rebuilds_stale_closure_rows`

## Implementation

`protocol.rs::SyncMessage::ViewUpdate`; `protocol.rs::ResultMemberEntry`; `node/views.rs::NodeState::view_update_for_query_binding_with_peer_payload_inventory_and_plan`
