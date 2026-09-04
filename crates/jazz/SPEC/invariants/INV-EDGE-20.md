# INV-EDGE-20

- Status: target
- Coverage: untested

## Invariant

A worker's internal authority-result source identity MUST carry the exact upstream membership for every Edge/Global handoff. It MUST NOT create a second public projection or cause one transaction to arrive through conflicting view bundles.

## Enforced by (tests)

`jazz::tests::browser_relay_durability::browser_client_local_full_returns_immediately_then_reconciles_upstream`; `jazz::tests::browser_relay_durability::reopened_browser_tab_hydrates_from_worker_authority_state`

## Implementation

`jazz/src/db/peer_connection.rs::PeerConnection::subscribe`; `jazz/src/peer/publication.rs::PeerState::rehydrate_query_maintained_subscription_view`
