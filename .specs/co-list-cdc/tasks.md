# Implementation Tasks

## Tasks

- [ ] 1. Add the public CDC API and exported types — `currentCursor(...)`, `subscribeChanges(...)`, `ChangeCursor`, `ChangeSubscription`, `ChangeBatch`, `Change`, `ChangePath`, `ChangeHandle`, `CursorError` — exported from `jazz-tools`, following the "Public API" and "Public Types" sections of [design.md](/Users/guidodorsi/workspace/jazz/.specs/co-list-cdc/design.md).
- [ ] 2. Implement `CursorCodec` with versioned cursor encoding and decoding, root ID validation, and stable resolve fingerprint normalization, following the "CDC Subsystem" and "Cursor Internals" sections of [design.md](/Users/guidodorsi/workspace/jazz/.specs/co-list-cdc/design.md).
- [ ] 3. Implement `GraphSnapshot` on top of `SubscriptionScope` so it captures reachable descendants, parent-child edges, logical path metadata, and per-node session frontiers, following the "CDC Subsystem", "Reusing Existing Jazz Primitives", and "Best-Effort Descendant Failures" sections of [design.md](/Users/guidodorsi/workspace/jazz/.specs/co-list-cdc/design.md).
- [ ] 4. Implement `ReplayEngine` to merge post-cursor transactions across the active graph in global transaction order using a priority queue seeded from each node's eligible transactions, following the "Core Flow" and "Current-Membership Replay" sections of [design.md](/Users/guidodorsi/workspace/jazz/.specs/co-list-cdc/design.md).
- [ ] 5. Implement `Translator` support for `CoList`, `CoMap`, `CoPlainText`, and `CoFeed` transactions so it emits only meaningful change records, including synthetic `list-replace` collapse and filtering of no-op or losing writes, following the "Semantic Change Families", "Meaningful Changes Only", and "Translation Rules" sections of [design.md](/Users/guidodorsi/workspace/jazz/.specs/co-list-cdc/design.md).
- [ ] 6. Implement `ChangeSubscription` to coordinate initial graph loading, optional backlog replay, live tailing via `subscribeToGraphDiff`, listener delivery, cursor advancement, and best-effort descendant handling for `subscribeChanges(...)` and `currentCursor(...)`, following the "CDC Subsystem" and "Core Flow" sections of [design.md](/Users/guidodorsi/workspace/jazz/.specs/co-list-cdc/design.md).
- [ ] 7. Apply the five targeted `SubscriptionScope` modifications described in the "SubscriptionScope Modifications" section of [design.md](/Users/guidodorsi/workspace/jazz/.specs/co-list-cdc/design.md): make `resolve` a public readonly field; add `readonly parentId` to child scopes; add a `frontier` accessor; add an `allDescendants()` iterator; add `subscribeToGraphDiff`.
- [ ] 8. Add integration tests per the "Integration Test Suite" section of this file.
- [ ] 9. Add focused unit tests for cursor codec behavior, resolve fingerprint normalization, graph snapshot extraction, frontier application on resume, transaction merge ordering, `list-replace` translation collapse, and `CoMap` no-op filtering, following the "Unit Test Coverage" section of [design.md](/Users/guidodorsi/workspace/jazz/.specs/co-list-cdc/design.md).

---

## Integration Test Suite

File: `packages/jazz-tools/src/tools/tests/cdc.test.ts`

Each test group below corresponds to a distinct behavioral concern.
Tests use the same setup pattern: create a real Jazz `LocalNode` (or equivalent test helper), create and populate CoValues, run the CDC API against them, and assert on the collected batches.

---

### Group 1 — `currentCursor`

**1.1** `currentCursor` returns a non-empty opaque string for a loaded root.

**1.2** `currentCursor` on a root with resolved descendants encodes frontiers for all reachable nodes (verified by decoding the cursor internally and checking the node count matches the resolve depth).

**1.3** `currentCursor` with a `CoList` root, no `resolve` — encodes only the root frontier.

**1.4** `currentCursor` with a `CoMap` root — works identically to a `CoList` root; returns a valid cursor.

**1.5** `currentCursor` with a `CoFeed` root — returns a valid cursor.

**1.6** `currentCursor` with a `CoPlainText` root — returns a valid cursor.

**1.7** `currentCursor` with a multi-level `resolve` (root → list → map → text) — encodes frontiers for all four levels.

**1.8** `currentCursor` called twice on the same graph with no mutations in between — both cursors are equal.

**1.9** `currentCursor` called after a mutation — the new cursor differs from the pre-mutation cursor.

---

### Group 2 — `subscribeChanges` without a cursor (tail mode)

**2.1** Changes made before `subscribeChanges` is called are not delivered (tail-from-now default).

**2.2** A change made after `subscribeChanges` is called is delivered in the next batch.

**2.3** `subscription.cursor()` returns `undefined` before the first batch is delivered.

**2.4** `subscription.cursor()` returns a non-`undefined` value after the first batch is delivered.

**2.5** `handle.cursor()` inside the listener equals `batch.cursor`.

**2.6** `subscription.cursor()` after delivery equals the last `batch.cursor`.

**2.7** `unsubscribe()` stops further delivery: mutations made after `unsubscribe()` are not delivered.

**2.8** Calling `unsubscribe()` twice does not throw.

**2.9** Multiple sequential mutations each arrive as separate batches (or coalesced if in the same transaction — the important thing is none are skipped).

---

### Group 3 — `subscribeChanges` with a cursor (replay + tail)

**3.1** All mutations made after a cursor are replayed in order before any live-tail batches arrive.

**3.2** Mutations made during replay (while replay is in progress) are buffered and delivered after replay drains — no mutation is skipped.

**3.3** No batch is delivered twice at the replay/tail boundary.

**3.4** A mutation made before the cursor is not replayed.

**3.5** After replay drains, subsequent live mutations are delivered in real time.

**3.6** `subscription.cursor()` is `undefined` until the first batch is delivered, even when a cursor was passed in.

**3.7** `subscription.cursor()` after replay equals the cursor of the last replayed batch.

**3.8** Resuming from the last-delivered cursor via a fresh `subscribeChanges` call produces zero replay batches when no new mutations have been made.

**3.9** Resuming from an intermediate cursor replays only the delta between that cursor and the current frontier.

**3.10** Replay order is deterministic: two independent calls with the same cursor against the same graph produce identical batch sequences.

---

### Group 4 — Change kinds: `CoList`

**4.1** Appending an item to a `CoList` emits a `list-insert` with the correct `index` and `value`.

**4.2** Deleting an item from a `CoList` emits a `list-delete` with the correct `index`.

**4.3** Deleting a ref-valued item from a `CoList` emits a `list-delete` with a `deletedRefId`.

**4.4** A single transaction that inserts at position _i_ and deletes the previously occupying slot — same net index set — emits a synthetic `list-replace`, not a separate insert + delete.

**4.5** A transaction that both inserts and deletes but changes the net set of occupied indices emits a `list-insert` and a `list-delete` separately, not a `list-replace`.

**4.6** `list-insert` at position 0 (prepend) emits `index: 0`.

**4.7** Multiple inserts in a single transaction each produce a separate `list-insert` change in index order.

---

### Group 5 — Change kinds: `CoMap`

**5.1** Setting a key on a `CoMap` emits a `map-set` with the correct `key` and `value`.

**5.2** Setting a key to a CoValue ref emits a `map-set` with `value: { refId: "..." }`.

**5.3** Deleting a key from a `CoMap` emits a `map-delete` with the correct `key`.

**5.4** Setting a key to the same value it already holds (no effective change) emits nothing.

**5.5** A losing concurrent write — a write from a session whose timestamp is earlier than an already-applied write for the same key — emits nothing.

**5.6** Deleting a key that does not exist emits nothing.

**5.7** A transaction that sets multiple keys emits one `map-set` per effectively changed key.

---

### Group 6 — Change kinds: `CoPlainText`

**6.1** Inserting characters into a `CoPlainText` emits a `text-change` whose `text` field reflects the full new string.

**6.2** Deleting characters from a `CoPlainText` emits a `text-change` with the updated string.

**6.3** A transaction that produces no net change to the rendered string (e.g. concurrent insert at the same position that resolves identically) emits nothing.

---

### Group 7 — Change kinds: `CoFeed`

**7.1** Appending an entry to a `CoFeed` emits a `feed-append` with the correct `value`.

**7.2** Appending a ref-valued entry to a `CoFeed` emits a `feed-append` with `value: { refId: "..." }`.

**7.3** Multiple feed appends in sequence each produce a separate `feed-append` batch.

---

### Group 8 — Descendant changes and `ChangePath`

**8.1** A change on a root-level `CoMap` value emits with `parentId: undefined` and `path: [{ kind: "root", coValueId: rootId }]`.

**8.2** A change on a direct `CoMap` child of a `CoList` emits with `parentId` equal to the list's CoValue ID and a `path` of length 2: `[root, list-item]`.

**8.3** A change on a grandchild `CoPlainText` (root `CoList` → `CoMap` child → `CoPlainText` field) emits with `parentId` equal to the `CoMap`'s ID and a `path` of length 3: `[root, list-item, map-key]`.

**8.4** Two changes on different descendants in the same mutation cycle each carry their own distinct `coValueId` and `parentId`.

**8.5** The `path` array's first element always has `kind: "root"` and `coValueId` equal to the root.

**8.6** A `map-key` path segment carries `coValueId` when the value is a CoValue ref (nested node), and omits it for scalar values.

---

### Group 9 — Current-membership replay

**9.1** A node that was reachable at cursor time and is still reachable at `subscribeChanges` time is replayed correctly.

**9.2** A node that was detached from the graph before `subscribeChanges` is called is silently skipped — no changes from that node appear in replay.

**9.3** A node that was attached after the cursor was taken (no frontier entry) is replayed from the beginning.

**9.4** Intermediate mutations on a node that was attached, then detached before `subscribeChanges`, are not replayed.

**9.5** The structural change (e.g. `list-delete`) that caused a node's detachment is included in replay if it occurred after the cursor.

**9.6** A newly attached node that had mutations before it was attached (from a prior life under a different parent) is replayed from the beginning of its own history.

---

### Group 10 — Cursor compatibility

**10.1** Passing a cursor with a `rootId` that does not match the `id` argument throws a synchronous `CursorError` before any async work begins.

**10.2** Passing a cursor with a `resolveFingerprint` that does not match the `resolve` option throws a synchronous `CursorError`.

**10.3** Passing a cursor with an unknown `version` field throws a synchronous `CursorError`.

**10.4** A cursor produced with `{ a: true, b: true }` is accepted by a `subscribeChanges` call that passes `{ b: true, a: true }` — key order must not matter.

**10.5** `CursorError` is not routed to the listener — it is thrown to the caller before the subscription is established.

**10.6** A cursor produced by `currentCursor` is accepted by a `subscribeChanges` call with the same `schema`, `id`, and `resolve`.

---

### Group 11 — Best-effort descendants

**11.1** If a descendant CoValue is not yet loaded (unavailable), changes to other descendants in the same graph are still delivered.

**11.2** If a descendant CoValue is not authorized for the `loadAs` agent, changes to authorized descendants are still delivered.

**11.3** An unavailable descendant does not cause the subscription to emit an error or stop.

**11.4** When an unavailable descendant later becomes available, its subsequent changes are delivered.

---

### Group 12 — Cursor advancement and persistence

**12.1** `batch.cursor` advances monotonically: each new batch's cursor is strictly "later" than the previous one (verified by resuming from each cursor in sequence and observing no re-delivery).

**12.2** A subscription resumed from cursor _C₁_ produces the same next batch as the subscription that originally produced _C₁_, given the same subsequent mutations.

**12.3** Encoding a cursor, then decoding it, then resuming from the decoded cursor produces the same replay as resuming from the original cursor.

**12.4** After a full round-trip (build index → persist cursor → restart → resume from cursor), no already-delivered batches are re-delivered.

---

### Group 13 — Multi-author and concurrent writes

**13.1** Writes from two different sessions are both delivered.

**13.2** Concurrent writes from two sessions to the same `CoMap` key: the losing write is filtered out; only the winning write is delivered as a `map-set`.

**13.3** Replay order for transactions from two sessions is deterministic and matches `compareTransactions` total order: `(madeAt, sessionID, txIndex, branch, coValueId)`.

**13.4** Two independent calls to `subscribeChanges` with the same cursor against the same multi-author graph produce identical batch sequences (stable replay).

**13.5** Changes from a session that rejoins (reconnects and sends new transactions) are delivered after reconnection.

---

### Group 14 — Root schema variants

**14.1** `subscribeChanges` with a `CoList` root delivers `list-insert` / `list-delete` for mutations to the root list.

**14.2** `subscribeChanges` with a `CoMap` root delivers `map-set` / `map-delete` for mutations to the root map.

**14.3** `subscribeChanges` with a `CoFeed` root delivers `feed-append` for new entries appended to the root feed.

**14.4** `subscribeChanges` with a `CoPlainText` root delivers `text-change` for edits to the root text.

**14.5** A `CoMap` root with a nested `CoList` field delivers both root-level `map-set` changes and descendant `list-insert` / `list-delete` changes when both are mutated.
