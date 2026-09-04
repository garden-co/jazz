# INV-API-4

- Status: now
- Coverage: ✓

## Invariant

When `ReadOpts.local_updates == LocalUpdates::Immediate`, the effective read tier MUST be at least `DurabilityTier::Local`; when it is `Deferred`, the effective read tier MUST be exactly `ReadOpts.tier`.

## Enforced by (tests)

`jazz::db::tests::read_opts_default_and_effective_tier_preserve_local_update_contract`

## Implementation

`jazz/src/db.rs::effective_read_tier`
