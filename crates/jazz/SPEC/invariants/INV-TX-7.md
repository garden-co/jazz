# INV-TX-7

- Status: now
- Coverage: ✓

## Invariant

A commit unit whose `tx_id.time.physical_ms()` exceeds the authority admission clock by more than `SKEW_TOLERANCE_MS` MUST be rejected as `RejectionReason::ClientClockTooFarAhead` and MUST NOT leave visible version rows.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::commit_unit_forward_skew_rejects_and_client_cleans_up`

## Implementation

`jazz/src/node/ingest.rs::NodeState::ingest_commit_unit_once`
