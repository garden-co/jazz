# INV-QUERY-16

- Status: now
- Coverage: ✓

## Invariant

Same-drain result churn MUST be folded by net output-row outcome: enter-then-leave sends no stale add, leave-then-reenter replaces the old entry, and same-tx retract/assert churn sends no update.

## Enforced by (tests)

`jazz::peer::tests::incremental_query_result_set_drops_enter_then_leave_same_drain_cycle`; `jazz::peer::tests::incremental_query_result_set_keeps_leave_then_reenter_same_drain_cycle`

## Implementation

`peer.rs::PeerState::query_update_from_deltas`
