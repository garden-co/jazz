# INV-SYNC-26

- Status: now
- Coverage: ✓

## Invariant

A receiver detecting a referenced version without its body MUST be able to request exactly those `(table, row_uuid, tx_time, tx_node_id)` payloads, and the server MUST serve them subject to ordinary read policy. The repair vocabulary and server/client repair helpers are implemented and activated for declared known-state subscriptions.

## Enforced by (tests)

`jazz::node::tests::harness::row_version_fetch_returns_authorized_versions_and_omits_unauthorized_rows`; `jazz::node::tests::harness::declared_known_state_view_update_repairs_withheld_row_version_body`; `jazz::node::tests::harness::known_state_rehydrate_skips_known_bodies_and_repairs_missing_payload`

## Implementation

`protocol.rs::SyncMessage::FetchRowVersions`; `protocol.rs::SyncMessage::RowVersionPayloads`; `peer.rs::PeerState::serve_row_versions`; `node/mod.rs::NodeState::missing_known_state_row_version_refs`; `node/mod.rs::NodeState::apply_row_version_payloads_for_requests`; `db.rs::PeerConnection::tick`
