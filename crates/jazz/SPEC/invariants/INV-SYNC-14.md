# INV-SYNC-14

- Status: now
- Coverage: ✓

## Invariant

A read-policy revocation MUST remove the affected row from future settled subscription result sets but MUST NOT require redaction of previously delivered local copies.

## Enforced by (tests)

`jazz::node::tests::policies_rls::owner_transfer_removes_settled_result_set_without_redacting_local_copy`; `jazz::node::tests::policies_rls::composed_read_policy_grants_and_revokes_incrementally`

## Implementation

`node/views.rs::view_update_for_query_binding_with_peer_payload_inventory_and_plan`; `peer.rs::apply_outgoing_view_update_result_set`
