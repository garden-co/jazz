# INV-DISC-4

- Status: prov
- Coverage: untested

## Invariant

commit/fate/view ingestion must be idempotent and conflict-detecting. Duplicate relay commit units compare transaction payload and canonical versions, then no-op if identical (ingest.rs lines 318-332); duplicate authority commits return existing fate or reject conflicting payload (ingest.rs lines 391-425); apply_fate in four-tier tests applies the same fate twice (four_tier.rs lines 122-125); stale pending cannot regress accepted fate (fate_regressions.rs lines 33-63). This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
