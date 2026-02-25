# Query Subscription Rejection Propagation — TODO (MVP)

## Status

Proposed.

## Problem

When the sync server rejects a query subscription (missing read permission, query compilation failure on a relay), it sends `SyncPayload::Error(QuerySubscriptionRejected { query_id, reason })` back to the client. The client's `sync_manager::inbox` logs it to stderr and discards it. The error never reaches the subscription callback, the SubscriptionsOrchestrator, or the React `useAll` hook.

The downstream plumbing already exists and is untriggered:

- `useAll` reducer has `entry_error` action and `rejected` status
- `SubscriptionsOrchestrator` has `onError` listener callbacks
- `useAllSuspense` throws on rejected state for ErrorBoundary

## Design

Seven layers need a small change each across three platform bindings (WASM, NAPI, React Native). No new concepts; every layer already has a near-identical pattern to follow.

### Layer 1: inbox.rs — route rejection from server

`process_from_server` currently logs `SyncPayload::Error` and discards it. Change to:

1. Match `QuerySubscriptionRejected { query_id, reason }` specifically.
2. Relay to originating downstream clients via `query_origin` (same pattern as the `QuerySettled` arm three lines above).
3. Push `(query_id, reason)` to a new `pending_query_rejections` vec on SyncManager for local subscription processing.

Other `SyncError` variants keep the existing `eprintln!` behaviour.

### Layer 2: SyncManager — pending_query_rejections field

Add `pending_query_rejections: Vec<(QueryId, String)>` to `SyncManager`. Add `take_pending_query_rejections()` following the `take_pending_query_settled()` pattern.

### Layer 3: QueryManager::process() — drain rejections into failed_subscriptions

After step 7a (QuerySettled processing), drain `pending_query_rejections`. For each `(query_id, reason)`:

- Convert to `QuerySubscriptionId(query_id.0)` (they share the same u64).
- If a local subscription exists, remove it and push a `QuerySubscriptionFailure { subscription_id, reason }`.
- If no local subscription exists (relay forwarded it but has no local sub), skip silently.

The existing `take_failed_subscriptions()` path carries the failure upward to RuntimeCore.

### Layer 4: RuntimeCore — error callback on subscriptions

Add an optional error callback to `SubscriptionState`:

```
on_error: Option<SubscriptionErrorCallback>
```

Where `SubscriptionErrorCallback` mirrors `SubscriptionCallback` but takes a `String` reason (with the same `Send` / `!Send` cfg split for native vs WASM).

In `immediate_tick()`, the existing `subscription_failures` loop currently drops regular subscriptions and logs. Change to: if `on_error` is `Some`, invoke it with the failure reason before removing the subscription.

### Layer 5: RuntimeCore subscribe methods — accept error callback

Add `on_error: Option<SubscriptionErrorCallback>` parameter to `subscribe_impl`. Thread it through `subscribe` and `subscribe_with_settled_tier` (both the native and WASM cfg variants).

### Layer 6: Platform bindings — on_error parameter

All three platform bindings need the same change: accept an error callback and thread it to `RuntimeCore::subscribe_with_settled_tier`.

**WASM** (`crates/jazz-wasm/src/runtime.rs`) — Add `on_error: Option<Function>` to the `#[wasm_bindgen] subscribe()` method. Wrap it into a `SubscriptionErrorCallback` closure that calls the JS function with the reason string.

**NAPI** (`crates/jazz-napi/src/lib.rs`) — Add `on_error: Option<JsFunction>` to `subscribe()`. Convert to a `ThreadsafeFunction` and wrap into a `SubscriptionErrorCallback`.

**React Native** (`crates/jazz-rn/rust/src/lib.rs`) — Extend the `SubscriptionCallback` trait with `on_error(&self, reason: String)`. Update the UniFFI binding and the TypeScript adapter (`jazz-rn-runtime-adapter.ts`) to accept and forward the callback.

### Layer 7: TypeScript — wire error callback to SubscriptionsOrchestrator

Three files, all additive:

**client.ts** — `subscribe()` and `subscribeInternal()` accept `onError?: (reason: string) => void`. Pass it as the `on_error` argument to `this.runtime.subscribe(...)`.

**db.ts** — `subscribeAll()` accepts `onError?: (error: unknown) => void`. Passes it through to `client.subscribe()`. Update the `DbLike` interface to include the optional parameter.

**subscriptions-orchestrator.ts** — In `ensureEntryForKey()`, pass an `onError` callback to `this.db.subscribeAll()` that:

1. Sets `entry.state` to `{ status: "rejected", data: undefined, error }`.
2. Calls `entry.rejectfulfilled(error)`.
3. Notifies all current listeners via `listener.onError?.(error)`.

This is the same logic as the existing synchronous `catch` block, just triggered asynchronously.

## Data flow summary

```
Server sends SyncPayload::Error(QuerySubscriptionRejected { query_id, reason })
  │
  ▼
inbox.rs: process_from_server
  ├─ relay to downstream clients via query_origin
  └─ push to SyncManager.pending_query_rejections
       │
       ▼
     QueryManager::process()
       └─ drain rejections → QuerySubscriptionFailure
            │
            ▼
          RuntimeCore::immediate_tick()
            └─ invoke SubscriptionState.on_error(reason)
                 │
                 ▼  (WASM boundary)
               JazzClient.subscribe onError callback
                 │
                 ▼
               Db.subscribeAll onError callback
                 │
                 ▼
               SubscriptionsOrchestrator
                 ├─ entry.state = { status: "rejected", error }
                 └─ listener.onError(error)
                      │
                      ▼
                    useAll reducer: { type: "entry_error", error }
                      └─ state = { status: "rejected", error }
```

## Non-goals

- Retry logic or automatic resubscription after rejection.
- Surfacing other `SyncError` variants (PermissionDenied, SessionRequired, CatalogueWriteDenied) through this path. Those are write errors with different routing needs.
