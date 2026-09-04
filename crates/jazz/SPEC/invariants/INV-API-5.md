# INV-API-5

- Status: now
- Coverage: ✓

## Invariant

`ReadOpts::default()` MUST be `{ tier: DurabilityTier::Local, local_updates: LocalUpdates::Immediate, propagation: Propagation::Full }`.

## Enforced by (tests)

`jazz::db::tests::read_opts_default_and_effective_tier_preserve_local_update_contract`

## Implementation

`jazz/src/db.rs::ReadOpts::default`
