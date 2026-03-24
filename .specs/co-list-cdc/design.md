# Design

## Overview

This feature adds first-class CDC support for any collaborative CoValue root in Jazz.
The public API is a pair of standalone exported functions designed for durable consumers rather than UI-only reactivity.

The design supports:

- replayable opaque cursors
- transaction-batched delivery
- nested descendant tracking through Jazz `resolve` queries
- best-effort descendant resolution
- current-membership replay

This design intentionally satisfies the approved "history + live" requirement through `subscribeChanges(...)` plus `currentCursor(...)`.
There is no separate pull-style `readChanges(...)` API in v1.

This design does not include any search or example-app work.

## Vocabulary

| Term | Definition |
|---|---|
| **frontier** | A per-session map of `SessionID → txIndex` recording the highest transaction index already seen for that session. Used to resume replay without re-processing already-delivered transactions. |
| **cursor** | An opaque string that encodes the frontier of every tracked node at a point in time. Passed back to `subscribeChanges` to resume from that checkpoint. |
| **backlog** | Transactions that were written after the cursor was taken but before `subscribeChanges` is called. Replayed in causal order before live tail begins. |
| **tail** | The live-tail mode: delivering new transactions as they arrive, after any backlog replay is complete. A subscription without a cursor starts directly in tail mode. |
| **replay** | Re-processing historical transactions from each node's frontier up to the current head, in merged causal order across all nodes in the graph. |
| **graph** | The set of CoValues reachable from the root via the `resolve` query. Includes the root itself and all resolved descendants at any depth. |
| **graph diff** | A set of IDs that entered (`added`), left (`removed`), or received new transactions (`changed`) in the current update cycle. |
| **current-membership replay** | Replay only covers nodes that are reachable at the time `subscribeChanges` is called. Nodes detached before that point are silently skipped. |
| **node** | A single CoValue within the active graph. Each node tracks its own frontier for incremental replay. |
| **batch** | A `ChangeBatch`: one transaction's worth of semantic changes, plus author, timestamp, and a cursor checkpoint. |

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
- per-node session frontiers (`knownState.sessions`)

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
- the active graph (maintained incrementally via `subscribeToGraphDiff`)
- the last emitted cursor

### Reusing Existing Jazz Primitives

The design intentionally reuses current internals instead of creating a second graph loader:

- `SubscriptionScope` provides deep `resolve` traversal and best-effort descendant handling.
- `CoValueCoreSubscription` and `SubscriptionScope` already model how nested updates bubble through a resolved graph.
- `CoValueCore.getValidSortedTransactions(...)` already exposes the sorted, decrypted transaction stream needed for replay.
- `knownState.sessions` already provides the per-session frontier needed for resume.
- existing inspector transaction helpers provide good precedent for semantic translation of list and text changes.

### SubscriptionScope Modifications

The CDC subsystem surfaces friction with `SubscriptionScope`'s current API.
The following targeted modifications make the CDC implementation cleaner, more efficient, and free of unsafe casts.
They are listed from least to most architectural, but all are worth making.

#### 1. Make `resolve` a public readonly field

`resolve` is currently private.
The CDC works around this with an unsafe cast:

```ts
const resolve = (scope as unknown as { resolve: ResolveValue }).resolve;
```

Making it `readonly` on the constructor eliminates the cast with zero behavior change.

```ts
constructor(
  public node: LocalNode,
  public readonly resolve: RefsToResolve<any>,  // was private
  public id: string,
  ...
)
```

#### 2. Add `parentId` to child scopes

Child scopes are self-contained and do not know who created them.
The CDC must therefore thread `parentId` manually through every recursive traversal of the scope tree.

Adding `readonly parentId: string | undefined` to the constructor — set by `loadChildNode` when it creates child scopes — makes scope trees self-describing. To preserve backward compatibility, it should be appended after the existing optional parameters rather than inserted in the middle:

```ts
constructor(
  public node: LocalNode,
  public readonly resolve: RefsToResolve<any>,
  public id: string,
  public schema: RefEncoded<CoValue>,
  ...
  public readonly parentId: string | undefined,  // new, last param
)
```

This removes external parent tracking from `GraphSnapshot` and the recursive snapshot builders.

#### 3. Add a `frontier` accessor

The CDC accesses the session frontier for each node via four levels of raw-layer indirection:

```ts
current.$jazz.raw.core.knownState().sessions
```

A convenience accessor on `SubscriptionScope` isolates the raw-layer access to one place:

```ts
get frontier(): Record<SessionID, number> | undefined {
  if (this.value.type !== CoValueLoadingState.LOADED) return undefined;
  return this.value.value.$jazz.raw.core.knownState().sessions as Record<SessionID, number>;
}
```

#### 4. Add an `allDescendants()` iterator

`graphSnapshotFromScope` in the CDC reimplements a recursive walk of `scope.childNodes` that the scope tree already represents.
A flat iterator over all reachable descendant scopes eliminates this duplication and replaces the hand-rolled recursive visitor:

```ts
*allDescendants(): IterableIterator<SubscriptionScope<CoValue>> {
  for (const child of this.childNodes.values()) {
    yield child;
    yield* child.allDescendants();
  }
}
```

With this in place, `graphSnapshotFromScope` becomes a single flat loop over `scope.allDescendants()`, and `loadAttachedSubtree` can read the already-loaded child scope from `childNodes` instead of opening a redundant new `SubscriptionScope`.

#### 5. Add a root-level graph-diff subscription

The highest-leverage change.
The root scope aggregates child-change information from its entire subtree and fires a single, deduplicated diff per update cycle:

```ts
subscribeToGraphDiff(
  callback: (diff: {
    added: Set<string>;
    removed: Set<string>;
    changed: Set<string>;
  }) => void
): () => void
```

- `added`: IDs that entered the reachable graph this cycle (newly attached subtrees)
- `removed`: IDs that left the reachable graph this cycle (detached subtrees)
- `changed`: IDs whose transactions advanced this cycle (value updates)

`loadChildren()` already computes which children were added and removed — it iterates `idsToLoad` vs `childNodes.keys()` — but currently discards that information after returning a bare `hasChanged: boolean`.
`subscribeToGraphDiff` surfaces it, aggregated upward through the tree to the root.

With this primitive, `ChangeSubscriptionController` maintains its active graph incrementally by applying the diff directly, and no longer needs a separate `#currentSnapshot` or the sync/prune passes that go with it.

### Replay-to-Live-Tail Handoff

After the replay loop drains all eligible backlog transactions, `ChangeSubscription` switches into live tail mode using the same `SubscriptionScope` that was already open.
To avoid double delivery at the boundary, the engine records the frontier at which replay ended (the set of per-session sequence numbers already consumed) and ignores any live-tail notifications whose transactions fall at or below that frontier.
Transactions that arrive during replay are buffered and processed in order after the replay loop completes.

### Core Flow

#### 1. `currentCursor(...)`

`currentCursor(...)`:

- loads the root with `bestEffortResolution: true`
- builds a `GraphSnapshot` from the resulting subscription tree
- serializes the per-node session frontiers into an opaque cursor

#### 2. `subscribeChanges(...)` without a cursor

`subscribeChanges(...)` without a cursor:

- loads the root graph with `bestEffortResolution: true`
- computes a baseline cursor immediately
- emits only future changes

This keeps the no-cursor case cheap and aligns with the approved tail-from-now default.

#### 3. `subscribeChanges(...)` with a cursor

`subscribeChanges(...)` with a cursor:

- decodes and validates the cursor
- loads the root graph with the requested `resolve` via `SubscriptionScope`
- applies the saved per-node frontiers to nodes currently in the graph; nodes no longer reachable are silently skipped; nodes new since the cursor have no saved frontier and are replayed from the beginning
- replays all transactions after each node's frontier in merged order
- continues tailing live changes via `subscribeToGraphDiff`

### Current-Membership Replay

The replay node set is determined once from the graph that is currently reachable via `SubscriptionScope` at the time `subscribeChanges(...)` is called.
Only those nodes are eligible for replay; the set does not change mid-replay.

Nodes that were attached between the cursor and now but have since been detached are not in the current graph and are silently skipped.
Nodes that were attached after the cursor and are still present have no saved frontier; they are replayed from the beginning.

This is correct for the primary use cases — search indexing, audit logs, event pipelines — because a detached node will already have produced a structural change event (`list-delete`, `map-delete`) that the consumer can act on.
Its intermediate mutations while attached are irrelevant once it is gone.

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
 * `coValueId` identifies the CoValue that changed.
 * `parentId` is the immediate parent in the resolved graph; it is absent only
 * for changes on the root CoValue itself.
 * `index` reflects the logical list index after the transaction is applied.
 */
type Change =
  | {
      kind: "list-insert";
      coValueId: string;
      parentId?: string;
      index: number;
      value: JsonValue | { refId: string };
    }
  | {
      kind: "list-delete";
      coValueId: string;
      parentId?: string;
      index: number;
      deletedRefId?: string;
    }
  | {
      kind: "list-replace";
      coValueId: string;
      parentId?: string;
      index: number;
      value: JsonValue | { refId: string };
      replacedRefId?: string;
    }
  | {
      kind: "map-set";
      coValueId: string;
      parentId?: string;
      key: string;
      value: JsonValue | { refId: string };
    }
  | {
      kind: "map-delete";
      coValueId: string;
      parentId?: string;
      key: string;
    }
  | {
      kind: "text-change";
      coValueId: string;
      parentId?: string;
      text: string;
    }
  | {
      kind: "feed-append";
      coValueId: string;
      parentId?: string;
      value: JsonValue | { refId: string };
    };
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
      key: "title",
      value: "Buy milk",
    },
  ],
};
```

### Cursor Internals

The cursor stays opaque publicly.
Internally it is a versioned encoded structure containing only:

- `version`
- `rootId`
- `resolveFingerprint`
- `frontiers`: a map from CoValue ID to per-session sequence numbers (`Record<string, Record<SessionID, number>>`)

Graph topology — which nodes are reachable, their parent edges — is not stored in the cursor.
It is re-derived from the live `SubscriptionScope` on resume.
Frontiers for nodes no longer in the current graph are ignored; nodes present in the current graph with no frontier entry are replayed from the beginning.

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

// The graph is fixed at replay start from current reachability.
// Frontiers from the cursor are applied to matching nodes.
const graph = buildActiveGraphFromScope(rootScope, cursor);

while (true) {
  const next = pickNextTransaction(graph);
  if (!next) break;

  const { node, tx } = next;
  const changes = translateTransaction(node, tx);
  advanceFrontier(node, tx);

  if (changes.length === 0) continue;

  const batch = { cursor: encodeCursor(graph), ...metadata(tx), changes };
  listener(batch, handle);
}

// Switch to live tail via subscribeToGraphDiff — no separate mode needed.
rootScope.subscribeToGraphDiff((diff) => {
  applyDiff(graph, diff);
  processNewTransactions(graph, listener, handle);
});
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
- detached descendants are not replayed (current-membership replay)
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
test("does not replay a descendant that was detached before subscribeChanges is called", async () => {
  const cursor = await currentCursor(TaskList, listId, {
    resolve: { $each: { notes: true } },
  });

  // Entry is removed after the cursor is taken. It is no longer in the
  // current graph when subscribeChanges runs, so nothing about it replays.
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

- cursor encoding and decoding (frontiers-only payload)
- resolve fingerprint normalization
- graph snapshot extraction from a `SubscriptionScope` via `allDescendants()`
- frontier application on resume: nodes present get their saved frontier, nodes absent are skipped, new nodes start from zero
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
