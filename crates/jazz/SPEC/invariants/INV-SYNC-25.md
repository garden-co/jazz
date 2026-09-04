# INV-SYNC-25

- Status: now
- Coverage: ✓

## Invariant

A stream served under known-state dedup followed by its repair responses MUST be observationally equivalent to the same stream served without dedup.

## Enforced by (tests)

`jazz::node::tests::harness::known_state_rehydrate_skips_known_bodies_and_repairs_missing_payload`; `jazz::node::tests::harness::declared_known_state_view_update_repairs_withheld_row_version_body`; `jazz::node::tests::harness::slow_known_state_declaration_skips_exact_local_versions_only`

## Implementation

`node/mod.rs::NodeState::missing_known_state_row_version_refs`; `node/mod.rs::NodeState::apply_row_version_payloads_for_requests`; `db.rs::PeerConnection::tick`; `node/views.rs::view_update_for_maintained_result_members`
