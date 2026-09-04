# INV-SYNC-18

- Status: now
- Coverage: ✓

## Invariant

An edge acting as mergeable fate authority MUST defer fate assignment until the relevant permission-scope subscription has settled for the writer and affected tables.

## Enforced by (tests)

`jazz::tests::four_tier::edge_defers_mergeable_fate_until_permission_scope_settles`

## Implementation

`peer.rs::ingest_edge_mergeable_commit_unit`; `peer.rs::permission_scopes_settled_for`; `peer.rs::drain_deferred_edge_fates`
