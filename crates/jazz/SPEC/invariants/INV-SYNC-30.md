# INV-SYNC-30

- Status: now
- Coverage: ✓

## Invariant

A durable `settled_through` cursor is known-state possession for payload dedup and repair, never an active authority receipt. Edge/Global subscription settlement and one-shot coverage require a confirming `ViewUpdate` from the selected continuously active upstream. A fresh settled one-shot MUST receive confirmation for its exact current usage-site `SubscriptionKey`; an update for a detached predecessor MUST NOT satisfy it even when shape, binding, and options are equal. Disconnect, restart, edge switch, or a conflicting nonselected-upstream update invalidates that receipt immediately.

## Enforced by (tests)

`jazz::db::tests::{edge_global_settlement_requires_a_fresh_current_connection_view_receipt, nonselected_upstream_update_demotes_selected_receipt_before_publication, one_shot_edge_global_coverage_requires_current_authority_after_reconnect, stale_old_upstream_epoch_cannot_settle_after_edge_switch_or_fallback, restarted_client_reuses_durable_cursor_but_waits_for_current_authority_receipt}`; `jazz::db::tests::peer_connection::authorization_scope::late_detached_view_update_does_not_cover_equal_shape_reattachment`

## Implementation

`jazz/src/db.rs::AuthorityViewReceipts`; `jazz/src/db.rs::apply_pending_authority_view_updates`; `jazz/src/db.rs::subscription_is_settled`; `jazz/src/db/subscriptions.rs::Db::query_attachment_is_covered`
