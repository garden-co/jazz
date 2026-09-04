# INV-API-28

- Status: now
- Coverage: ✓

## Invariant

Permission advice is a three-valued dry run: `Allowed`/`Denied` are definitive only when issued by the ready, history-complete serving authority for the authenticated link subject; a client-local, offline, incomplete-history, not-permissions-ready, unavailable, or timed-out path MUST yield `Unknown`. Advice MUST NOT create or alter rows/versions, reserve a subsequent ordinary optimistic mutation, or disclose supporting rows, policy reasons, or hidden dependency facts. Each response MUST correlate to its fresh request id; cancellation removes the waiter, late/replayed responses MUST NOT resolve another request (including after reopen), and serving-side replay dedup MUST be bounded.

## Enforced by (tests)

`jazz::db::tests::{permission_advice_uses_authenticated_link_identity_without_mutating,permission_advice_is_unknown_until_authority_permissions_are_ready,partial_replica_cannot_act_as_permission_advice_authority,permission_advice_response_wire_cannot_carry_policy_rows_or_reasons,cancelled_permission_advice_ignores_late_or_replayed_response_ids,dropped_permission_advice_is_not_sent_and_reopened_nodes_use_fresh_ids}`; `packages/jazz-tools/src/runtime/native-runtime/runtime.test.ts`

## Implementation

`jazz/src/db.rs::Db::request_permission_advice`; `jazz/src/db.rs::PeerConnection::tick`; `jazz/src/protocol.rs::SyncMessage::PermissionAdviceRequest`; `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts::NativeRuntimeAdapter::withPermissionAdviceTimeout`
