# INV-DISC-6

- Status: prov
- Coverage: untested

## Invariant

the implementation must preserve structural column taxonomy. Wire payloads carry VersionRecord data and not local/global-derived currentness (protocol.rs lines 110-140); local current rows use groove current graphs except Global, which routes to global-current storage (node/mod.rs lines 714-760); upstream state is represented on TransactionRecord as fate, global_time, and durability (tx.rs lines 130-149). This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
