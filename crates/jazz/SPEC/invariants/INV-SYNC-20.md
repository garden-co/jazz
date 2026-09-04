# INV-SYNC-20

- Status: now
- Coverage: ✓

## Invariant

Incremental query view updates MUST be observationally equivalent to a full rehydrate for the same canonical program instance, including enter/leave churn within a single drain cycle and closure-row replacement.

## Enforced by (tests)

`jazz::peer::tests::incremental_query_result_set_drops_enter_then_leave_same_drain_cycle`; `jazz::peer::tests::incremental_query_result_set_keeps_leave_then_reenter_same_drain_cycle`; `jazz::peer::tests::incremental_query_result_set_rebuilds_stale_closure_rows`; `jazz::peer::tests::incremental_query_result_sets_match_full_rehydrate_after_seeded_commits`

## Implementation

`peer.rs::query_update_from_deltas`; `peer.rs::rebuild_closure_contributions_from_update`; `peer.rs::apply_contribution_add`; `peer.rs::apply_contribution_remove`
