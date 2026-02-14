# Reconnection Re-Subscription (MVP v2)

## Goal

Ensure query forwarding converges in both cases:

1. Local/downstream subscriptions become active before any upstream is connected.
2. Upstream disconnects, local/downstream subscriptions change while offline, then upstream reconnects.

This is a prototype protocol. We do not require wire/backward compatibility with earlier behavior.

## Core Invariants

1. A subscription is local desired state, not a best-effort transient network event.
2. After every successful upstream (re)connect, upstream receives the current desired subscription set.
3. Unsubscribed queries are not replayed.
4. Convergence must not depend on timing between "subscribe" and "connect."

## MVP Protocol

### Stable stream identity

- Client stores the server-assigned `client_id` from `Connected`.
- Reconnects to `/events` with `?client_id=<last_client_id>` to resume identity.

### Runtime attach/detach lifecycle

- On stream `Connected`:
  - (Re)attach upstream server in runtime.
  - Attachment triggers full object sync + replay of active local/downstream query subscriptions.
- On stream disconnect/error:
  - Detach upstream server from runtime.
  - Local writes/subscriptions continue locally while offline.

### Why this converges

- Offline window: subscriptions are kept in runtime desired state.
- Reconnect: `add_server` replay re-asserts all active subscriptions.
- Timing/order cannot drop query forwarding, because replay is anti-entropy.

## Retry / Backoff

- Stream reconnect uses exponential backoff with jitter.
- Backoff applies to stream re-establishment, not to local desired-state retention.

## Server-side behavior (MVP)

- Do not immediately drop client state on event-stream disconnect.
- Reconnect with same `client_id` continues that logical client state.

## Future Optimization Stub (not in this MVP)

Current MVP uses full replay on each reconnect. Later we can optimize with a resumable diff handshake:

- Client sends `(client_id, desired_state_version, desired_state_digest, count)`.
- Server replies `InSync` or `ReplayRequired`.
- Client sends full replay only on mismatch.

Correctness requirement stays the same: full replay must remain the fallback anti-entropy path.
