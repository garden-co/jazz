# Design

## Overview

This feature adds change tracking support for any CoValue root in Jazz at the cojson layer.
The public API is a pair of standalone functions that enable durable consumers (e.g. incremental search indexes) to detect which CoValues were added, removed, or changed since a previous checkpoint.

The design supports:

- structured cursors encoding per-node session frontiers
- depth-1 graph tracking (root + direct refs)
- network-efficient resume via `known` messages (no content transferred for unchanged values)
- implicit subscriptions for live-tail push

This design intentionally limits scope to:

- cojson layer only (jazz-tools integration comes later)
- depth-1 resolve (root + direct refs, no recursive descendant tracking)
- graph-diff output only (ID sets, not semantic field-level changes)

## Vocabulary

| Term | Definition |
|---|---|
| **frontier** | A per-session map of `SessionID -> txIndex` recording the highest transaction index already seen for that session. |
| **cursor** | A structured record mapping each tracked node to its frontier at a point in time. Passed back to `subscribeToChanges` to resume from that checkpoint. |
| **graph** | The root CoValue plus all CoValues it directly references (depth-1 refs). |
| **changes message** | A set of IDs that entered (`added`), left (`removed`), or received new transactions (`changed`) in the current update cycle. |
| **node** | A single CoValue within the tracked graph. |

## Public API

Export two functions from `cojson`:

```ts
type ChangesMessage = {
  added: Set<RawCoID>;
  removed: Set<RawCoID>;
  changed: Set<RawCoID>;
};

type ChangesCursor = {
  rootId: RawCoID;
  frontiers: Record<RawCoID, Record<SessionID, number>>;
};

type ChangesSubscription = {
  unsubscribe(): void;
  /** Returns the cursor after the last emitted ChangesMessage,
   *  or undefined if no message has been emitted yet. */
  cursor(): ChangesCursor | undefined;
};

function loadChangesCursor(
  node: LocalNode,
  rootId: RawCoID,
): Promise<ChangesCursor>;

function subscribeToChanges(
  node: LocalNode,
  rootId: RawCoID,
  cursor: ChangesCursor,
  listener: (changes: ChangesMessage, cursor: ChangesCursor) => void,
): ChangesSubscription;
```

`loadChangesCursor(...)` loads the root, extracts its direct ref IDs, and snapshots all their frontiers into a `ChangesCursor`.

`subscribeToChanges(...)` always requires a cursor. There is no "tail from now" mode. The consumer builds their initial state using normal Jazz APIs, then calls `loadChangesCursor` to snapshot the frontier.

### Usage Pattern

```ts
// First run: build index from current state, then snapshot
const tasks = await loadCoList(node, listId);
for (const task of tasks) { index.add(task); }
const cursor = await loadChangesCursor(node, listId);
persist(JSON.stringify(cursor));

// Subsequent runs: resume from cursor
const cursor = JSON.parse(savedCursor);
const sub = subscribeToChanges(node, listId, cursor, (changes, newCursor) => {
  for (const id of changes.added) { index.add(await load(id)); }
  for (const id of changes.removed) { index.remove(id); }
  for (const id of changes.changed) { index.update(await load(id)); }
  persist(JSON.stringify(newCursor));
});
```

## Architecture

### Resume Flow

When `subscribeToChanges` is called with a cursor:

**Phase 1 — Validate and send (synchronous)**

1. Read per-node frontiers from `cursor.frontiers`
2. Validate cursor: check `cursor.rootId` matches `rootId`. Throw `CursorError` synchronously if invalid.
3. Return `ChangesSubscription` immediately. All remaining work is async.

**Phase 2 — Subscribe (async)**

4. Subscribe to root and all cursor items via `known` messages with the cursor's knownState. Each outgoing `known` message includes `header: true` (all cursor items were previously loaded and had headers) and the node's frontier from the cursor.
5. The server treats each incoming `known` message as an implicit subscription (calls `peer.combineWith()`). For each node:
   - **Unchanged:** silence (nothing sent, no content transferred)
   - **Changed:** server sends `content` with delta transactions only

**Phase 3 — Emit initial diff (async, gated on root)**

6. Wait for the root to be loaded (it receives content if changed, or stays at cursor state if unchanged).
7. From root's current state, derive current direct ref IDs.
8. Compare current refs with cursor's ID set:
   - IDs in current but not cursor -> `added`
   - IDs in cursor but not current -> `removed`
   - IDs in both that received `content` from the server -> `changed`
9. Subscribe to `added` IDs via `known` with empty sessions (load everything).
10. Emit `ChangesMessage` with `{ added, removed, changed }`.

Content for cursor-intersection items that arrives after step 10 (e.g. slow server responses) is emitted in subsequent live-tail diffs.

### Why `known` Messages

Using `known` messages (instead of `load`) to subscribe gives the client control over the loading strategy:

- With storage: CoValueCore may have local data more recent than the cursor. The sync protocol handles delta resolution automatically.
- Without storage: content is pulled in full from the server.
- Either way, subscriptions are created implicitly (the server calls `peer.combineWith()` on any incoming `known` message).

The server only sends `content` for nodes that have actually changed. Unchanged nodes produce zero network traffic beyond the initial `known` message from the client.

### Live Tail

After the resume diff is emitted, all nodes are subscribed. The server pushes content on future changes via the normal subscription mechanism.

- **Root changes:** re-derive direct ref set from root's current state, compare with the ref set from the most recent emission (or the cursor's ID set, for the first cycle) -> `added`/`removed`
- **Descendant changes:** content arrives for a tracked node -> `changed`
- **Batching:** multiple changes arriving in quick succession are coalesced into a single `ChangesMessage` per microtask

The cursor is updated after each emission. The listener receives the new cursor.

### `unsubscribe` Cleanup

Calling `unsubscribe()` on the `ChangesSubscription` stops emitting `ChangesMessage` to the listener and tears down the internal tracking state. The underlying CoValueCore subscriptions created via `known` messages remain tied to the peer connection lifecycle and are cleaned up when the connection closes (standard Jazz behavior).

### Network Cost

For a graph of N nodes where K changed since the cursor:

- **Outgoing:** N small `known` messages (knownState only, ~100-200 bytes each)
- **Incoming:** K `content` responses (delta only) + silence for N-K unchanged nodes
- **Live tail:** proportional to rate of change, not total graph size

### `loadChangesCursor` Flow

1. Load root CoValue via normal `load`
2. Extract direct ref IDs from root's current state (CoMap ref values, CoList ref items, CoFeed ref entries)
3. Subscribe to each ref via `known` with empty sessions to obtain its knownState. Only the frontier (`knownState().sessions`) is needed — the full decrypted content is not required.
4. Return `ChangesCursor` with `rootId` and all collected frontiers

If the root cannot be loaded (unauthorized, missing), `loadChangesCursor` rejects the returned promise. If a referenced child cannot be loaded, it is omitted from the cursor (best-effort). On resume, an omitted child that is still referenced by the root will appear in `added`.

### Cursor Structure

The `ChangesCursor` is a plain record:

- `rootId`: the root CoValue ID this cursor was created for
- `frontiers`: `Record<RawCoID, Record<SessionID, number>>` — per-node session frontiers for root + direct refs

All nodes in the cursor are assumed to have `header: true` (they were previously loaded). When sending `known` messages on resume, the outgoing message sets `header: true` for all cursor items.

Graph topology (which nodes are reachable, parent edges) is not stored in the cursor. It is re-derived from the root on resume. Frontiers for nodes no longer referenced by the root are ignored. Nodes referenced by the root with no frontier entry are treated as new (`added`) and loaded from the beginning.

Serialization is the consumer's responsibility (e.g. `JSON.stringify`/`JSON.parse`).

When a cursor is passed to `subscribeToChanges` and validation fails (wrong `rootId`), the function throws a synchronous `CursorError`.

## Error Handling

### Root Unavailable

If the root cannot be loaded (unauthorized, missing) during `subscribeToChanges`, the error is delivered to the listener as a rejected-style callback (or an `onError` handler — TBD during implementation). No partial results. Since cursor validation is synchronous (Phase 1), post-validation errors like root unavailability are async and cannot be thrown from the `subscribeToChanges` call site.

### Cursor Validation

- Wrong `rootId` -> throw `CursorError` synchronously

### Stale Cursor Items

Cursor contains IDs no longer referenced by root. They appear in the `removed` set. The `known` message is still sent but the result is irrelevant since the item left the graph.

### New Items

Root references IDs not in the cursor. They appear in the `added` set. Subscribed via `known` with empty sessions (load everything).

### Connection Drops

Peer reconnects. Peer reconciliation re-establishes subscriptions using existing CoValueCore knownState (standard Jazz behavior, nothing CDC-specific).

### Concurrent Root Mutations During Resume

Content arrives for root during resume. Re-derive ref set and emit additional diff in the next batch.

## Testing Strategy

### Integration Tests

- Resume with cursor where nothing changed -> empty diff, no content transferred
- Resume with cursor where some items changed -> correct `changed` set, only delta transferred
- Resume with items added since cursor -> correct `added` set
- Resume with items removed since cursor -> correct `removed` set
- Live tail: mutate a descendant -> `changed` emitted
- Live tail: add item to root -> `added` emitted
- Live tail: remove item from root -> `removed` emitted
- Cursor from wrong root -> `CursorError`

### Unit Tests

- Ref extraction from CoMap, CoList, CoFeed root types
