# INV-BVIEW-17

- Status: now
- Coverage: ✓

## Invariant

Trusted replication MAY carry complete cross-branch-key commit units; untrusted selected delivery MUST NOT reveal unauthorized sibling versions, tables, branch keys, payloads, or counts merely because they share a transaction.

## Enforced by (tests)

`jazz::peer::tests::maintained_subscription_view_exclusive_delta_ships_view_scoped_partial_bundle`; `jazz::peer::tests::maintained_subscription_view_policy_view_exclusive_delta_ships_identity_scoped_partial_bundle`; `jazz::node::tests::harness::view_scoped_cardinality_survives_reopen_and_upgrades_to_complete_payload`

## Implementation

`protocol.rs::VersionBundleScope`; `node/views.rs::NodeState::version_bundle_for_maintained_view_versions_with_tx`; `node/views.rs::NodeState::ingest_view_bundle`
