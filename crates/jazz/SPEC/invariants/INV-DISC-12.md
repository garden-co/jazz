# INV-DISC-12

- Status: prov
- Coverage: untested

## Invariant

benchmarks are discipline gates that report deterministic counters plus timing ratios, but appendix A should not quote dirty-tree numbers. Sync benchmark emits JSON fields for seed/config, fate RTT, view refresh, version bundle/ref counts, reject counts, and parked-orphan counters (sync.rs lines 300-318). Validation benchmark config uses clients/rows/commits/hot-row percent/seed (validation.rs lines 28-48). This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
