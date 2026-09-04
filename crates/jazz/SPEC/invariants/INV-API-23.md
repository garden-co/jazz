# INV-API-23

- Status: now
- Coverage: partial

## Invariant

A client binding tick driver MUST classify `Db::tick()` failures. A recoverable protocol condition MUST NOT terminate the driver; the driver MUST continue through its documented repair or reconnect path with bounded backoff. A fatal failure, or exhausted recovery, MUST stop the driver and be surfaced to the caller as an error rather than appearing as a stalled sync operation.

## Enforced by (tests)

`jazz::tools::client::tests::fatal_tick_driver_failure_is_reported_to_callers`; partial: proves fatal failure surfacing; recoverable-failure classification and reconnect continuation remain uncovered

## Implementation

`jazz/src/tools/client.rs::classify_tick_driver_error`; `jazz/src/tools/client.rs::recover_tick_driver_error`; `jazz/src/tools/client.rs::ClientDbInner::ensure_tick_driver_running`
