# Implicit Session for Policy Enforcement

## Problem

`JazzClient.query()` and `JazzClient.subscribe()` do not pass a session to the
WASM runtime. Without a session the query compiler omits the
`PolicyFilterNode`, so every row passes through unfiltered regardless of the
app's permission rules.

`SessionClient` (via `client.forSession(...)`) already threads session through
`queryInternal` / `subscribeInternal`, and the WASM layer handles it correctly.
The gap is in the top-level `JazzClient` methods that callers use by default:
React hooks, `SubscriptionsOrchestrator`, and direct `client.query()` /
`client.subscribe()` calls.

The same omission propagates through `Db.subscribeAll` and
`SubscriptionsOrchestrator`, which are the paths used by `createJazzClient`.

## Fix: two layers

### 1. Client-side (root cause)

Resolve the session eagerly in the `JazzClient` constructor by parsing the JWT
that was provided in the `AppContext`. Use that resolved session as the default
in `query()` and `subscribe()`. Thread it through `SubscriptionsOrchestrator`
and `Db.subscribeAll` so the React integration path benefits too.

| File                            | Change                                                                 |
| ------------------------------- | ---------------------------------------------------------------------- |
| `client-session.ts`             | Export `resolveJwtSession`                                             |
| `client.ts`                     | Add `resolvedSession` field; default `query()` and `subscribe()` to it |
| `db.ts`                         | Accept optional `session` in `subscribeAll`; call `subscribeInternal`  |
| `subscriptions-orchestrator.ts` | Accept `session` in constructor; forward to `subscribeAll`             |
| `create-jazz-client.ts`         | Pass resolved session to `SubscriptionsOrchestrator`                   |

### 2. Server-side (defence in depth)

The server also falls back to the connection-level session when a
`QuerySubscription` arrives with `session: None`. In
`process_pending_query_subscriptions()`, after schema resolution, resolve
`session_for_policy` from `sub.session` or, when absent, from
`sync_manager.get_client(sub.client_id).session` (set during JWT
authentication on the WebSocket handshake). All downstream uses (graph
compilation, scope registration, upstream forwarding, subscription storage)
use `session_for_policy` instead of `sub.session`.

| File                | Change                                                                                                 |
| ------------------- | ------------------------------------------------------------------------------------------------------ |
| `server_queries.rs` | Resolve `session_for_policy` from `sub.session` or `sync_manager.get_client().session`; use throughout |

The server's connection-level session uses a hashed principal ID
(`external:<hash>`) that differs from the raw JWT `sub` claim the client
resolves. This means the server-side fallback adds a `PolicyFilterNode`
whose `session.user_id` does not match rows inserted using the raw claim,
silently filtering all of them out. This is the preferred failure mode: fail
closed (zero results) rather than fail open (bypass policies). The ID format
mismatch is a separate concern to reconcile later.
