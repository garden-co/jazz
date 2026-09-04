# INV-API-19

- Status: now
- Coverage: ✓

## Invariant

An upstream `PeerConnection::tick` MUST send each unannounced usage-site subscription by first sending `SyncMessage::RegisterShape` once per shape and then `SyncMessage::Subscribe` for each usage id; serving peers MUST group matching `(shape_id, binding_id, opts)` subscriptions behind one maintained coverage group.

## Enforced by (tests)

`jazz::db::tests::db_sync_surface_round_trips_subscription_to_client`

## Implementation

`jazz/src/db.rs::PeerConnection::tick`; `jazz/src/db.rs::binding_values_in_param_order`
