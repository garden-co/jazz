# INV-API-34

- Status: now
- Coverage: ✓

## Invariant

An edge outbox MUST retain an edge-accepted upload until an authenticated terminal rejection or an `Accepted` receipt carrying both Global durability and an authority-assigned `GlobalTime` for that `TxId` arrives directly from the currently admitted upstream fate authority; a featureless/unnegotiated link, local acceptance, hydrated state, staged/replayed updates, and receipts from detached or superseded authorities MUST NOT release it.

## Enforced by (tests)

`jazz::db::tests::peer_connection::admission_and_fates::{outbox_release_requires_current_admitted_authority_receipt, featureless_upstream_cannot_release_routed_edge_outbox}`; `jazz::db::tests::node_runtime::pending_global_state_does_not_complete_remote_wait_or_prune_upload`

## Implementation

`jazz/src/db/peer_connection.rs::PeerConnection::tick`; `jazz/src/db.rs::PendingUpload`; `jazz/src/db.rs::queue_pending_upload_in`
