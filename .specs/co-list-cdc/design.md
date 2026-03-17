# Design

## Overview

This feature adds first-class CDC support for any collaborative CoValue root in Jazz.
The public API is a pair of standalone exported functions designed for durable consumers rather than UI-only reactivity.

The design supports:

- replayable opaque cursors
- transaction-batched delivery
- nested descendant tracking through Jazz `resolve` queries
- best-effort descendant resolution
- strict historical graph membership

This design intentionally satisfies the approved "history + live" requirement through `subscribeChanges(...)` plus `currentCursor(...)`.
There is no separate pull-style `readChanges(...)` API in v1.

This design does not include any search or example-app work.

## Architecture / Components

### Public API

Export two standalone CDC functions from `jazz-tools`:

- `currentCursor(schema, id, options): Promise<ChangeCursor>`
- `subscribeChanges(schema, id, options, listener): ChangeSubscription`

Both functions accept any `AnyCoreCoValueSchema` as their first argument.
This makes the API schema-agnostic: a `CoMap` root, a `CoList` root, a `CoFeed` root, or any other collaborative CoValue schema all work identically.

`currentCursor(...)` returns a baseline checkpoint for the current reachable graph.
`subscribeChanges(...)` is the only change-consumption API:

- without a cursor, it tails from subscription start
- with a cursor, it replays backlog after that cursor and then continues tailing

Both functions accept a `ChangeCursorOptions` / `ChangeSubscribeOptions` object (see Data Models).
Both functions accept a `loadAs` option consistent with `load(...)` and `subscribe(...)`.

### CDC Subsystem

Implement a small CDC subsystem in `jazz-tools` with these internal pieces:

- `CursorCodec`
- `GraphSnapshot`
- `ReplayEngine`
- `Translator`
- `ChangeSubscription`

`CursorCodec` is responsible for encoding and decoding opaque versioned cursors.
It validates cursor compatibility against:

- root CoValue ID
- normalized `resolve` fingerprint
- cursor version

`GraphSnapshot` is responsible for building the currently reachable graph from a root CoValue.
It captures:

- reachable CoValue IDs
- parent-child edges
- logical path segments
- per-node `knownState`
- unresolved descendants so replay can continue best-effort

`ReplayEngine` is responsible for replaying post-cursor transactions across the active graph in one merged order.
It uses existing `CoValueCore.getValidSortedTransactions(...)` and the existing transaction ordering in `CoValueCore.compareTransactions(...)`.

Cross-node interleaving: the engine maintains a priority queue seeded with the next eligible transaction from each reachable CoValue.
On each `popNextTransaction()` call it dequeues the globally earliest transaction (by `compareTransactions` ordering) and advances that node's cursor.
This produces a deterministic, causal merge order across all nodes in the graph.

`Translator` is responsible for turning raw transactions into semantic CDC batches.
It emits semantic changes, not raw transactions.
Transaction metadata stays internal to replay ordering and checkpoint progression.

`ChangeSubscription` is the long-lived coordinator.
It owns:

- the root `SubscriptionScope`
- the active graph snapshot
- the last emitted cursor
- replay state
- live tail state

### Reusing Existing Jazz Primitives

The design intentionally reuses current internals instead of creating a second graph loader:

- `SubscriptionScope` provides deep `resolve` traversal and best-effort descendant handling.
- `CoValueCoreSubscription` and `SubscriptionScope` already model how nested updates bubble through a resolved graph.
- `CoValueCore.getValidSortedTransactions(...)` already exposes the sorted, decrypted transaction stream needed for replay.
- `knownState.sessions` already provides the per-session frontier needed for resume.
- existing inspector transaction helpers provide good precedent for semantic translation of list and text changes.

### Replay-to-Live-Tail Handoff

After the replay loop drains all eligible backlog transactions, `ChangeSubscription` switches into live tail mode using the same `SubscriptionScope` that was already open.
To avoid double delivery at the boundary, the engine records the frontier at which replay ended (the set of per-session sequence numbers already consumed) and ignores any live-tail notifications whose transactions fall at or below that frontier.
Transactions that arrive during replay are buffered and processed in order after the replay loop completes.

### Core Flow

#### 1. `currentCursor(...)`

`currentCursor(...)`:

- loads the root with `bestEffortResolution: true`
- builds a `GraphSnapshot` from the resulting subscription tree
- serializes the graph membership and frontiers into an opaque cursor

#### 2. `subscribeChanges(...)` without a cursor

`subscribeChanges(...)` without a cursor:

- loads the root graph with `bestEffortResolution: true`
- computes a baseline cursor immediately
- emits only future changes

This keeps the no-cursor case cheap and aligns with the approved tail-from-now default.

#### 3. `subscribeChanges(...)` with a cursor

`subscribeChanges(...)` with a cursor:

- decodes and validates the cursor
- loads the root graph with the requested `resolve`
- reconstructs the active graph from the cursor and current scope
- replays all eligible transactions after the cursor
- switches into live tail mode using the same merged ordering rules

### Strict Historical Membership

Strict historical membership is required.

If a descendant was reachable at one time and later detached from the root graph:

- its earlier reachable changes must still replay
- its later detached changes must not replay

This means replay cannot be based only on current reachability.
The replay engine must update graph membership after every translated transaction.

When a transaction attaches a new descendant subtree:

- the subtree becomes eligible for subsequent CDC immediately after the attaching transaction

When a transaction detaches a subtree:

- that subtree stops being eligible immediately after the detaching transaction

### Best-Effort Descendant Failures

The root CoValue remains strict:

- if the root is unavailable or unauthorized, `currentCursor(...)` fails
- if the root is unavailable or unauthorized, `subscribeChanges(...)` routes through normal error handling

Descendants are best-effort:

- unauthorized descendants do not fail the whole stream
- unavailable descendants do not fail the whole stream
- unresolved descendants are skipped from the public stream

## Data Models

### Public Types

```ts
type ChangeCursor = string;

/**
 * Options accepted by `currentCursor(...)`.
 * `resolve` is typed relative to the schema passed as the first argument.
 */
type ChangeCursorOptions<S extends AnyCoreCoValueSchema> = {
  resolve?: ResolveQuery<S>;
  loadAs?: Account | AnonymousJazzAgent;
};

/**
 * Options accepted by `subscribeChanges(...)`.
 * `resolve` is typed relative to the schema passed as the first argument.
 */
type ChangeSubscribeOptions<S extends AnyCoreCoValueSchema> = {
  cursor?: ChangeCursor;
  resolve?: ResolveQuery<S>;
  loadAs?: Account | AnonymousJazzAgent;
};

/**
 * Passed as a second argument to the `subscribeChanges` listener on every call.
 * Provides access to the cursor after the current batch has been delivered,
 * without requiring the caller to hold a reference to the subscription object.
 */
type ChangeHandle = {
  cursor(): ChangeCursor;
};

type ChangeSubscription = {
  unsubscribe(): void;
  /**
   * Returns the cursor after the last delivered batch, or `undefined` if no
   * batch has been delivered yet.
   */
  cursor(): ChangeCursor | undefined;
};

type ChangeBatch = {
  cursor: ChangeCursor;
  coValueId: string;
  authorId: string;
  madeAt: Date;
  changes: Change[];
};

/**
 * All changes share `coValueId` (the CoValue that changed) and `path`.
 * `parentId` is the immediate parent in the resolved graph; it is absent only
 * for changes on the root CoValue itself.
 * `index` reflects the logical list index after the transaction is applied.
 */
type Change =
  | {
      kind: "list-insert";
      coValueId: string;
      parentId?: string;
      path: ChangePath;
      index: number;
      value: JsonValue | { refId: string };
    }
  | {
      kind: "list-delete";
      coValueId: string;
      parentId?: string;
      path: ChangePath;
      index: number;
      deletedRefId?: string;
    }
  | {
      kind: "list-replace";
      coValueId: string;
      parentId?: string;
      path: ChangePath;
      index: number;
      value: JsonValue | { refId: string };
      replacedRefId?: string;
    }
  | {
      kind: "map-set";
      coValueId: string;
      parentId?: string;
      path: ChangePath;
      key: string;
      value: JsonValue | { refId: string };
    }
  | {
      kind: "map-delete";
      coValueId: string;
      parentId?: string;
      path: ChangePath;
      key: string;
    }
  | {
      kind: "text-change";
      coValueId: string;
      parentId?: string;
      path: ChangePath;
      text: string;
    }
  | {
      kind: "feed-append";
      coValueId: string;
      parentId?: string;
      path: ChangePath;
      value: JsonValue | { refId: string };
    };

type ChangePath = Array<
  | { kind: "root"; coValueId: string }
  | { kind: "map-key"; key: string; coValueId?: string }
  | { kind: "list-item"; refId?: string; index?: number }
>;
```

Example:

```ts
const batch: ChangeBatch = {
  cursor: "opaque-cursor",
  coValueId: "co_zList456",
  authorId: "co_zAlice",
  madeAt: new Date("2026-03-17T12:00:00.000Z"),
  changes: [
    {
      kind: "map-set",
      coValueId: "co_zTask123",
      parentId: "co_zList456",
      path: [
        { kind: "root", coValueId: "co_zList456" },
        { kind: "list-item", refId: "co_zTask123", index: 2 },
        { kind: "map-key", key: "title" },
      ],
      key: "title",
      value: "Buy milk",
    },
  ],
};
```

### Cursor Internals

The cursor stays opaque publicly.
Internally it should be a versioned encoded structure containing:

- `version`
- `rootId`
- `resolveFingerprint`
- active reachable node IDs
- parent edges and enough path metadata to restore identity
- per-node `knownState.sessions`

The `resolveFingerprint` must be stable for semantically equivalent resolve queries.
The implementation should normalize keys before encoding (e.g. `{ b: true, a: true }` and `{ a: true, b: true }` must produce the same fingerprint).

When a cursor is passed to `subscribeChanges(...)` and validation fails (wrong `rootId`, mismatched `resolveFingerprint`, or unknown cursor `version`), the function must throw a synchronous `CursorError` before any async work begins.
This error is not routed to the listener — it is a programming error and the caller must handle it.

### Semantic Change Families

Supported semantic change kinds in v1:

- `list-insert`
- `list-delete`
- `list-replace`
- `map-set`
- `map-delete`
- `text-change`
- `feed-append`

These cover the resolved descendant types explicitly approved for v1:

- `CoList`
- `CoMap`
- `CoPlainText`
- `CoFeed`

`list-replace` is synthetic and should be emitted when a single transaction on a `CoList` contains both a positional insert (`app` or `pre` op) and a `del` op targeting the slot that the insert displaces, such that the net effect is replacing the value at one logical index rather than growing the list by one and shrinking it by one.
Concretely: if the transaction's ops, when applied, leave the same set of occupied indices as before but with a different value at one index, it is a replace.
If the set of indices changes the replace rule does not apply and the insert and delete are emitted separately.

### Meaningful Changes Only

The public CDC stream should contain only meaningful state changes.
Raw transaction artifacts that do not change the effective visible state must be filtered out before emission.

This rule applies across all translated CoValue kinds:

- For `CoMap`, emit `map-set` or `map-delete` only when the effective field value after the transaction differs from the effective field value before it.
- For out-of-order or losing `CoMap` writes that become no-ops under Jazz conflict resolution, emit nothing.
- For `CoList`, emit insert, delete, or replace only when the visible list content changes.
- For `CoPlainText`, emit `text-change` only when the final rendered string changes.
- For `CoFeed`, emit `feed-append` only for newly visible appended entries.

The translator should therefore operate on effective before/after state at the affected path, not on raw operations alone.

## Implementation Sketch

### Public API Sketch

```ts
const cursor = await currentCursor(TaskList, listId, {
  resolve: {
    $each: {
      notes: true,
      comments: { $each: true },
    },
  },
});

const sub = subscribeChanges(
  TaskList,
  listId,
  {
    cursor,
    resolve: {
      $each: {
        notes: true,
        comments: { $each: true },
      },
    },
  },
  (batch: ChangeBatch, handle: ChangeHandle) => {
    consume(batch);
    // handle.cursor() equals batch.cursor — convenient when the caller does
    // not want to hold a reference to `sub`.
    saveCheckpoint(handle.cursor());
  },
);
```

### Replay Engine Sketch

```ts
const rootScope = new SubscriptionScope(
  node,
  resolve,
  id,
  { ref: schema.getCoValueClass(), optional: false },
  false,
  true,
  unstableBranch,
);

const snapshot = graphSnapshotFromScope(rootScope);
const replay = createReplayEngine(cursor, snapshot);

while (replay.hasNext()) {
  const next = replay.popNextTransaction();
  const batch = translateTransaction(next, replay.currentGraph());

  listener(batch, handle);
  replay.advance(batch.cursor);
  replay.applyGraphChanges(batch);
}
```

### Translation Rules

```ts
function translateListTransaction(tx: DecryptedTransaction): Change[] {
  const changes = tx.changes as ListOpPayload<JsonValue>[];

  if (isReplacePattern(changes)) {
    return [toListReplace(changes, tx)];
  }

  return changes.flatMap((change) => {
    if (change.op === "app") return [toListInsert(change, tx)];
    if (change.op === "pre") return [toListInsert(change, tx)];
    if (change.op === "del") return [toListDelete(change, tx)];
    return [];
  });
}
```

```ts
function translateMapTransaction(
  before: RawCoMap,
  after: RawCoMap,
  tx: DecryptedTransaction,
): Change[] {
  const result: Change[] = [];

  for (const change of tx.changes as MapOpPayload<string, JsonValue>[]) {
    if (change.op === "set") {
      const prev = before.get(change.key);
      const next = after.get(change.key);

      if (Object.is(prev, next)) {
        continue;
      }

      result.push(toMapSet(change.key, next, tx));
    }

    if (change.op === "del") {
      const existedBefore = before.get(change.key) !== undefined;
      const existsAfter = after.get(change.key) !== undefined;

      if (!existedBefore || existsAfter) {
        continue;
      }

      result.push(toMapDelete(change.key, tx));
    }
  }

  return result;
}
```

## Examples

### Indexing with Orama

This example builds a full-text search index over a `TaskList` using Orama and keeps it current via `subscribeChanges`.
The Orama index and the CDC cursor are persisted together so the indexer can resume after a restart without rebuilding from scratch.

```ts
import { create, insert, remove, update, search } from "@orama/orama";
import persistence from "@orama/plugin-data-persistence";
import { currentCursor, subscribeChanges } from "jazz-tools";
import type { ChangeCursor } from "jazz-tools";

// --- Initial index build ---
// Called on first run when no saved state exists.
// Populates the index from current state and snapshots the frontier.
async function buildIndex(listId: string) {
  const db = await create({
    schema: { title: "string", body: "string" },
  });

  const oramaIds = new Map<string, string>();

  const tasks = await TaskList.load(listId, { resolve: { $each: true } });
  for (const task of tasks ?? []) {
    const id = await insert(db, {
      title: task.title ?? "",
      body: task.body ?? "",
    });
    oramaIds.set(task.id, id);
  }

  // Snapshot the current frontier so incremental indexing
  // only replays changes made after this point.
  const cursor = await currentCursor(TaskList, listId, {
    resolve: { $each: true },
  });

  return { db, oramaIds, cursor };
}

// --- Incremental indexer ---
// Subscribes to changes from a saved cursor and keeps the index current.
// Persists the index, the oramaIds map, and the cursor atomically after
// each batch so a crash at any point leaves all three in sync.
function startIndexer(
  db: Awaited<ReturnType<typeof create>>,
  oramaIds: Map<string, string>,
  listId: string,
  cursor: ChangeCursor,
) {
  return subscribeChanges(
    TaskList,
    listId,
    { cursor, resolve: { $each: true } },
    async (batch, handle) => {
      for (const change of batch.changes) {
        switch (change.kind) {
          case "list-insert": {
            if ("refId" in change.value) {
              const id = await insert(db, { title: "", body: "" });
              oramaIds.set(change.value.refId, id);
            }
            break;
          }

          case "list-delete": {
            const id = change.deletedRefId && oramaIds.get(change.deletedRefId);
            if (id) {
              await remove(db, id);
              oramaIds.delete(change.deletedRefId!);
            }
            break;
          }

          case "map-set": {
            const id = oramaIds.get(change.coValueId);
            if (id && (change.key === "title" || change.key === "body")) {
              await update(db, id, { [change.key]: String(change.value) });
            }
            break;
          }
        }
      }

      // Persist index, oramaIds map, and cursor together so they never drift.
      const snapshot = await persistence.save(db, "binary");
      await kv.set("orama:index", snapshot);
      await kv.set("orama:ids", JSON.stringify([...oramaIds]));
      await kv.set("orama:cursor", handle.cursor());
    },
  );
}

// --- Startup ---
// Restores persisted state on subsequent runs; falls back to a full build
// on first run or if storage is unavailable.
async function start(listId: string) {
  const savedIndex = await kv.get("orama:index");
  const savedIds = await kv.get("orama:ids");
  const savedCursor = (await kv.get("orama:cursor")) as ChangeCursor | undefined;

  let db: Awaited<ReturnType<typeof create>>;
  let oramaIds: Map<string, string>;
  let cursor: ChangeCursor;

  if (savedIndex && savedIds && savedCursor) {
    db = await persistence.load(
      await create({ schema: { title: "string", body: "string" } }),
      savedIndex,
    );
    oramaIds = new Map(JSON.parse(savedIds));
    cursor = savedCursor;
  } else {
    ({ db, oramaIds, cursor } = await buildIndex(listId));
  }

  return startIndexer(db, oramaIds, listId, cursor);
}

// --- Search ---
const results = await search(db, {
  term: "design system",
  properties: ["title", "body"],
  tolerance: 1,
});
```

**Key properties of this pattern:**

- The index snapshot, the `oramaIds` map, and the cursor are always persisted together, so they never drift out of sync across restarts.
- `buildIndex` + `currentCursor` is called exactly once; every subsequent restart uses `subscribeChanges` to replay only the delta.
- `list-insert` / `list-delete` track list membership; `map-set` handles field-level updates on each task.
- Swapping in a different root schema (e.g. a `CoMap` instead of a `CoList`) requires no changes to the indexer logic — only the schema argument and the `resolve` shape change.

## Testing Strategy

Integration tests are the primary validation strategy.
The behavior depends on graph loading, transaction replay, and live updates across nested CoValues, so narrow unit coverage alone would miss the most important regressions.

### Integration Test Coverage

Add integration tests for:

- `currentCursor(...)` on a reachable root graph
- `subscribeChanges(...)` without a cursor tailing future changes only
- `subscribeChanges(...)` with a cursor replaying backlog and then tailing live changes
- root CoMap, CoList, CoPlainText, and CoFeed as the root schema
- descendant `CoMap`, `CoList`, `CoPlainText`, and `CoFeed` semantics
- meaningful-change filtering for no-op or losing writes
- strict detach behavior for descendants removed from the root graph
- best-effort descendant authorization and availability failures
- cursor incompatibility for different root IDs or different resolve shapes throws `CursorError` synchronously
- stable replay ordering across merged branches

Example integration tests:

```ts
test("replays backlog and then tails live updates", async () => {
  const cursor = await currentCursor(TaskList, listId, {
    resolve: { $each: { comments: { $each: true } } },
  });

  createCommentAfterCursor();

  const batches: ChangeBatch[] = [];
  const sub = subscribeChanges(
    TaskList,
    listId,
    { cursor, resolve: { $each: { comments: { $each: true } } } },
    (batch) => batches.push(batch),
  );

  await waitFor(() => expect(batches.length).toBeGreaterThan(0));
  sub.unsubscribe();
});
```

```ts
test("does not emit changes from a detached descendant after removal", async () => {
  const cursor = await currentCursor(TaskList, listId, {
    resolve: { $each: { notes: true } },
  });

  removeEntryFromList();
  mutateDetachedEntry();

  const seen = await collectReplayFromCursor(TaskList, cursor);

  expect(seen).not.toContainEqual(
    expect.objectContaining({ coValueId: detachedEntryId }),
  );
});
```

```ts
test("works with a CoMap root", async () => {
  const cursor = await currentCursor(ProjectMap, projectId, {
    resolve: { tasks: { $each: true } },
  });

  mutateTask();

  const batches: ChangeBatch[] = [];
  const sub = subscribeChanges(
    ProjectMap,
    projectId,
    { cursor, resolve: { tasks: { $each: true } } },
    (batch) => batches.push(batch),
  );

  await waitFor(() => expect(batches.length).toBeGreaterThan(0));
  sub.unsubscribe();
});
```

### Unit Test Coverage

Add focused unit tests for:

- cursor encoding and decoding
- resolve fingerprint normalization
- graph snapshot extraction from a `SubscriptionScope`
- transaction merge ordering across multiple sessions
- `list-replace` translation collapse
- no-op `CoMap` filtering

## Assumptions

- V1 has no separate `readChanges(...)` API.
- V1 has no replay pagination or limit parameter.
- `subscribeChanges(...)` is callback-based, not async-iterator-based.
- CDC payloads are durable consumer payloads and do not expose live Jazz instances.
- Descendant failures are best-effort only below the root.
- Replay and translation may use transaction metadata internally, but the public `ChangeBatch` type does not expose it.
- This design supersedes the requirement document's open question about exact method names and exact event payload fields.
