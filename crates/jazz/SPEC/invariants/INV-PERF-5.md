# INV-PERF-5

- Status: now
- Coverage: untested

## Invariant

INV-PERF-5 incremental subscription state must converge to full rehydrate state for both filtered query bindings and whole-table current-row subscriptions. Identifiers: PeerSubscriptionState::result_member_set plus program facts, PeerState::query_update, PeerState::rehydrate_query, PeerState::current_rows_update. Test: incremental_query_result_sets_match_full_rehydrate_after_seeded_commits lines 2196-2284.

## Enforced by (tests)

NONE-FOUND

## Implementation
