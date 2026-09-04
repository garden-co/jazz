# INV-DISC-9

- Status: prov
- Coverage: untested

## Invariant

parked work must be observable and drained at quiescence. SyncMetrics currently tracks parked_orphans, parked_orphans_resolved, parked_incomplete, parked_incomplete_resolved, parked_catalogue_orphans, parked_catalogue_orphans_resolved, parked_catalogue_shapes, and parked_catalogue_shapes_resolved (node/mod.rs lines 1701-1720). Four-tier/threaded tests assert parked-orphan counts resolve (four_tier.rs lines 300-307; threaded_four_tier.rs lines 437-444). This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
