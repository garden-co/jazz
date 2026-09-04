# INV-DISC-5

- Status: prov
- Coverage: untested

## Invariant

time-like and state-lattice domains must use distinct types and monotone transitions. Identifiers: GlobalTime, TxTime, Fate, DurabilityTier. GlobalTime::tick explicitly packs physical milliseconds plus a logical counter; accepted core commits atomically persist their timestamp and committed frontier before applying DurabilityTier::Global (ingest.rs lines 513-533); errors include Error::NonMonotoneState and Error::ConflictingFate (node/mod.rs lines 2119-2127). This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
