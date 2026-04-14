# Unified CRUD API & Error Handling — TODO (MVP)

Simplify the user-facing mutation API by collapsing the current split between plain writes, `*Durable`, and `*Persisted`.

Today the surface makes callers choose between:

- immediate local writes: `insert` / `update` / `delete`
- wait-for-tier writes: `insertDurable` / `updateDurable` / `deleteDurable`
- replayable-handle writes: `insertPersisted` / `updatePersisted` / `deletePersisted`

Those are not three different user intents. They are one mutation intent with different settlement handling.

This spec makes the common API reflect that.

## Related

- [App Surface — Status Quo](../../status-quo/ts_client.md)
- [Opt-In Transactions, Replayable Reconciliation, and Strict Visibility](opt_in_transactions_replayable_reconciliation.md)

## Goals

- Expose one obvious CRUD API per operation.
- Keep direct writes local-first and immediately usable.
- Preserve tier-aware confirmation when the caller asks for it.
- Preserve good per-call async ergonomics for mutation errors.
- Add a restart-safe error channel for mutation failures that happen after the original caller is gone.
- Keep confirm-gated visibility tied to explicit transactions rather than ordinary writes.

## Non-Goals

- This spec does not redesign transaction APIs in detail.
- This spec does not change read-side durability or subscription APIs.
- This spec does not make every write transaction-shaped.

## Proposed API

### Direct writes

`Db` exposes one method per CRUD operation:

```ts
const inserted = db.insert(app.todos, { title: "Ship spec", done: false });
inserted.data.id;
await inserted.wait();

const updated = db.update(app.todos, todoId, { done: true });
await updated.wait();

const deleted = db.delete(app.todos, todoId);
await deleted.wait();
```

Each method accepts optional write options:

```ts
const inserted = db.insert(app.todos, { title: "Ship spec", done: false }, { tier: "edge" });
await inserted.wait();
```

Return shapes:

- `insert(...) -> { data: Row; wait: Promise<void> }`
- `update(...) -> { wait: Promise<void> }`
- `delete(...) -> { wait: Promise<void> }`

`wait` resolves when the requested tier confirms the write, or rejects if the write is rejected.

If no tier is provided, `wait` resolves immediately after local application.

### What gets removed

Remove these user-facing methods:

- `insertDurable`
- `updateDurable`
- `deleteDurable`
- `insertPersisted`
- `updatePersisted`
- `deletePersisted`

The user-facing API should not force callers to pick a different method just to decide whether they will `await` settlement.

## Semantics

### Direct writes stay visible immediately

Ordinary `insert` / `update` / `delete` remain direct visible writes.

That means:

- local state updates immediately
- ordinary queries and subscriptions keep their current local-first behavior
- asking for `tier` changes settlement behavior, not visibility mode

This is important. `tier` on an ordinary write means:

- "tell me when this write is confirmed at tier T"

It does **not** mean:

- "hide this write until tier T"

### Confirm-gated visibility is transaction-only

The runtime already treats strict accepted/rejected fate and optional pending-overlay behavior as transaction concerns rather than ordinary direct-write concerns.

This spec keeps that boundary sharp:

- ordinary writes are direct visible writes
- "visible on confirm" semantics are only available through transactions
- if an app wants authority-decided fate or confirm-gated visibility, it must opt into a transaction API

This keeps simple writes simple and avoids smuggling transaction semantics into every CRUD call.

## `wait` semantics

`wait` is the per-call ergonomic path for handling async mutation completion and async mutation failure.

Rules:

- if no tier was requested, `wait` resolves immediately
- if a tier was requested, `wait` resolves when replayable settlement reaches that tier or higher
- if the write later reaches a replayable terminal failure, `wait` rejects

The current replayable batch-settlement model is the source of truth for this decision. `wait` should resolve or reject from replayable settlement state, not only from live one-shot callbacks.

## Restart-safe late error reporting

Promises are still the best per-call API, but they cannot survive process restart, browser refresh, or app relaunch.

We therefore also need a separate global mutation error channel.

Sketch:

```ts
const unsubscribe = db.onMutationError((event) => {
  console.error(event);
});
```

This callback is for failures that may happen after the original write site is gone, especially:

- write issued while offline
- `wait` still pending
- app restarts
- runtime later reconnects
- authoritative rejection or missing settlement arrives for that earlier write

The callback should be fed from replayable batch-settlement state, so it can fire after reconnect and after restart.

Each event should include enough information to correlate the failure with real app behavior:

- `batchId`
- operation kind: `insert` / `update` / `delete`
- table name
- row id when applicable
- requested tier
- settlement kind
- settlement error details such as `code` and `reason`

A default logging implementation is acceptable, and framework bindings may layer toast/UI handling on top.

## Migration shape

Old:

```ts
const row = db.insert(app.todos, data);
await db.updateDurable(app.todos, row.id, patch, { tier: "edge" });
const pending = db.insertPersisted(app.todos, data, { tier: "global" });
await pending.wait();
```

New:

```ts
const inserted = db.insert(app.todos, data);
const durableUpdate = db.update(app.todos, inserted.data.id, patch, { tier: "edge" });
const globalWrite = db.insert(app.todos, data, { tier: "global" });

await durableUpdate.wait();
await globalWrite.wait();
```

If an app wants "visible on confirm" behavior, that write must use a transaction API rather than plain `db.insert(...)`:

```ts
const tx = db.beginTransaction(app.todos);
const pendingInsert = tx.insert(app.todos, data, { tier: "global" });
// `pendingInsert.data` is available, even though it's not visible for other queries/transactions
console.log({ pendingData: pendingInsert.data });
const committed = tx.commit();
// pendingInsert.wait and commited.wait are equivalent, since both promises are resolved once
// the core server confirms the write
await committed.wait();
```
