# INV-EDGE-20

- Status: now
- Coverage: ✓

## Invariant

A browser worker's internal relay-authority source identity MUST select authority membership only where cache re-evaluation changes meaning (a nonzero window or read-policy-scoped exact-ID read). A write-only policy does not select it. It MUST NOT create a second projection for ordinary Edge reads or cause one transaction to arrive through conflicting view bundles.

## Enforced by (tests)

`jazz::node::query_eval::tests::subscriptions::relay_authority_source_selection_requires_read_policy_for_exact_id`; `jazz::tests::browser_relay_durability::browser_worker_write_only_exact_edge_write_uses_one_ordinary_relay_projection`; `jazz::tests::browser_relay_durability::reopened_browser_tab_hydrates_from_worker_authority_state`; `packages/jazz-tools/tests/browser/db.private-read-gate.server.test.ts`

## Implementation

`jazz/src/node/query_eval.rs::NodeState::relay_edge_query_requires_authority_source`; `jazz/src/peer/publication.rs::PeerState::rehydrate_query_maintained_subscription_view`
