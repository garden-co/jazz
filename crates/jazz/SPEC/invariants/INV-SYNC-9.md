# INV-SYNC-9

- Status: now
- Coverage: untested

## Invariant

A receiver MUST reject a `ViewUpdate` that names a `peer_payload_inventory.complete_tx_payloads`, add, or remove transaction it lacks enough tx existence, row-version payload, complete-tx payload, or view-complete exclusive payload coverage to resolve for that subscription view.

## Enforced by (tests)

NONE-FOUND

## Implementation

`node/views.rs::apply_view_update`
