# INV-SYNC-7

- Status: now
- Coverage: partial: fact carrier exists, relation fact application pending

## Invariant

A `ViewUpdate` result set MUST be member-grained for result membership and typed-fact-grained for non-row program facts; it MUST NOT model subscription membership as a transaction-grained set. Ordinary current row entries are `ResultMemberEntry::Row(RealRowMemberEntry)` values with a `(table, row_uuid, content_tx_id)` projection. Synthetic payloads, relation/path, coverage, policy, predicate travel as typed `ProgramFactEntry` add/remove deltas. Relation facts MUST carry the dimensions needed by lowering (kind, versions, depth, edge id, branch, role, order, hole state) rather than requiring an opaque side channel.

## Enforced by (tests)

`jazz::node::tests::sync::view_updates_ship_current_versions_to_downstream_nodes`; `jazz::peer::tests::incremental_query_result_set_tracks_identical_cell_rewrite_tx_id`; `jazz::peer::tests::peer_state_sends_result_removes_after_deletes`

## Implementation

`protocol.rs::ResultMemberEntry`; `protocol.rs::ProgramFactEntry`; `protocol.rs::ResultMemberPayloadEntry`; `protocol.rs::RelationEdgeEntry`; `node/views.rs::view_update_for_query_binding_with_peer_payload_inventory_and_plan`; `peer.rs::PeerSubscriptionState::member_index`
