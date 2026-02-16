# Efficient Re-Subscribe (MVP Follow-up)

## Context

Current reconnect correctness relies on full replay of active query subscriptions when upstream is (re)attached. This is correct and must remain the fallback path.

## Goal

Reduce reconnect overhead (query replay traffic and server re-evaluation churn) when client/server subscription state is already in sync.

## Non-Goal

- Replacing full replay as correctness mechanism.
- Changing semantics of active subscription desired state.

## Proposed Optimization

Add a resumable subscription-state handshake before replay:

1. Client sends:
   - `client_id`
   - `desired_state_version`
   - `desired_state_digest`
   - `query_count`
2. Server compares with its stored state for that `client_id`.
3. Server responds:
   - `InSync` if digest/version match
   - `ReplayRequired` if mismatch or unknown state
4. Client sends full replay only on `ReplayRequired`.

## Correctness Invariants

- Full replay remains the mandatory fallback anti-entropy path.
- Unsubscribed queries must never be reintroduced by optimization shortcuts.
- A reconnect with stale or missing server state must converge to the same result as full replay.

## Validation

- Add tests for both `InSync` and `ReplayRequired` branches.
- Add reconnect churn tests showing no replay on true in-sync reconnect.
- Add stale-state tests proving replay fallback restores convergence.
