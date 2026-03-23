# History & Conflict Management Integration Tests

## Goal

Cover history (commit DAG) and conflict management (LWW resolution) with integration tests. Three test levels:

1. **Rust E2E tests** (`crates/jazz-tools/tests/history_conflict.rs`) — full-stack through `TestingServer` + `JazzClient`
2. **Rust ObjectManager isolated tests** (additions to `crates/jazz-tools/src/object_manager/tests.rs`) — DAG edge cases without sync overhead
3. **Browser tests** (`packages/jazz-tools/tests/browser/history-conflict.test.ts`) — full browser stack: WASM + Web Worker + OPFS + binary sync

Prep for future CRDTs: e2e tests black-box the resolution strategy, so when CRDTs arrive we add new ObjectManager tests and update e2e assertions.

## Background

- Every object contains named branches, each a DAG of immutable BLAKE3-addressed commits
- Concurrent edits create diverged tips (multiple frontier commits)
- Current conflict resolution: **LWW by timestamp** — highest timestamp tip wins
- `QueryManager::load_row_from_object_on_branch` sorts tips by timestamp, returns last (LWW selection)
- `ObjectManager::truncate_branch` prunes commits before specified tails (no existing tests for this)
- `ObjectManager::receive_commit` is idempotent (same CommitId = skip), accepts pre-built `Commit` with caller-controlled timestamps

## E2E Tests (`tests/history_conflict.rs`)

File must start with `#![cfg(feature = "test")]`.

Uses `TestingClient::builder()` pattern from `tests/support/mod.rs` (preferred over the older `make_client_context` pattern in `clients_sync.rs`). Helpers: `wait_for_query()`, `wait_for_subscription_update()`, `has_updated()`.

### Concurrent conflict reliability

Whether two `JazzClient` updates produce true DAG divergence (multiple tips) depends on timing — if sync is fast enough, the second client may parent its commit on the first's, creating a linear chain instead of a conflict. E2E tests assert **convergence** (both clients see the same final value) regardless of whether true divergence occurred. This makes the tests timing-tolerant while still exercising the full sync + resolution path. True DAG divergence and LWW tip selection are tested deterministically in the ObjectManager isolated tests (7-8) where we inject commits with controlled timestamps.

### Test 1: `concurrent_updates_resolve_to_lww_winner`

```text
alice ──create todo──► server ◄──update same todo── bob
         (both update title concurrently, no sync between writes)

         both query → see same winner (highest timestamp)
```

- Alice creates a todo, both clients sync and see it
- Alice updates title to "alice-edit", Bob updates title to "bob-edit" concurrently (no sync wait between writes)
- Both clients eventually converge: poll until both see the same title
- Assert both see identical final value (we don't assert which wins — just convergence)

### Test 2: `concurrent_creates_both_survive`

```text
alice ──create "buy milk"──► server ◄──create "buy eggs"── bob

         both query → see 2 todos
```

- Alice and Bob each create a todo concurrently
- Both eventually see 2 todos
- Verifies independent objects don't interfere during concurrent creation

### Test 3: `rapid_concurrent_updates_converge`

```text
alice ──update ×10──► server ◄──update ×10── bob
              (interleaved, no explicit sync waits)

         both query → same final value
```

- Alice creates a todo, synced to Bob
- Both fire 10 rapid updates to the same row's title (alice-N, bob-N)
- Both eventually converge to the same final title
- Stress test for LWW under high contention

### Test 4: `fresh_client_sees_lww_winner_after_conflict`

```text
alice + bob conflict on a todo ──► server
                                      │
              charlie connects fresh, queries
                                      │
                                      └──► sees LWW winner
```

- Alice and Bob create a conflict (concurrent updates to same todo)
- Wait for convergence between Alice and Bob
- Charlie connects fresh, queries — sees the same winner as Alice and Bob

### Test 5: `subscription_reflects_concurrent_update`

```text
alice subscribes to todos
bob updates a todo alice created
alice's subscription stream → sees update delta with bob's change
```

- Alice creates a todo, subscribes to the query
- Bob updates the todo
- Alice's subscription stream fires an update delta containing the change
- Uses `wait_for_subscription_update` + `has_updated` from support

### Test 6: `sequential_updates_preserve_latest`

```text
alice: create → update "v1" → update "v2" → update "v3"
bob: queries → sees "v3"
```

- Baseline: non-conflicting sequential updates always resolve to the last
- Alice creates and updates 3 times, waiting for EdgeServer settlement
- Bob queries and sees the final value

## ObjectManager Isolated Tests (additions to `src/object_manager/tests.rs`)

Direct `ObjectManager` + `MemoryStorage` tests, no async, no server.

### Test 7: `lww_selects_highest_timestamp_tip`

- Create object, add root commit
- Use `receive_commit` to inject two diverging commits with known timestamps (e.g. ts=100 and ts=200)
- Verify 2 tips exist
- Sort tips by timestamp (same logic as `load_row_from_object_on_branch`), assert the one with ts=200 wins
- This tests LWW selection logic, not just tip tracking

### Test 8: `lww_deterministic_on_equal_timestamps`

- Use `receive_commit` to inject two diverging commits with identical timestamps
- Verify the result is deterministic: call the selection multiple times, always same winner
- Documents the tie-breaking behavior: process-deterministic via Rust's stable sort over SmolSet iteration order (not canonically CommitId-ordered). Assert repeated calls return same result, don't assert which specific CommitId wins

### Test 9: `receive_commit_idempotent_during_conflict`

```text
root → a (tip)
root → b (tip)    ← 2 tips = conflict

receive_commit(b) again → no change, still 2 tips
```

- Build diverged state with 2 tips
- Re-receive one of the existing commits
- Assert tips unchanged (still exactly 2, same CommitIds)
- Assert no spurious subscription notifications from the replay

### Test 10: `truncate_with_diverged_tips`

```text
root → a1 → a2 (tip)
root → b1 → b2 (tip)

truncate(tails={a1, b1}) → root deleted, a1/b1 become tails, a2/b2 still tips
```

- Build a diamond-diverged DAG: root → a1 → a2, root → b1 → b2
- Truncate with tails = {a1, b1}
- Assert: root commit deleted, a1 and b1 are tails, a2 and b2 remain as tips
- Assert: total commits = 4 (a1, a2, b1, b2), root gone

### Test 11: `truncate_rejects_when_tip_not_descendant_of_tail`

```text
root → a (tip)
root → b (tip)

truncate(tails={a}) → error: b is not a descendant of a
```

- Build diverged state: root → a, root → b (2 tips)
- Truncate with tails = {a} only
- Assert: returns `TruncateError::TipBeforeTail(b)` because b is not a descendant of a
- Verifies the safety invariant

## Browser Tests (`packages/jazz-tools/tests/browser/history-conflict.test.ts`)

Same real stack as `worker-bridge.test.ts`: Chromium via Playwright, real jazz-wasm, real OPFS workers, real TestingServer. Two `Db` instances per test via `createSyncedDb`. Uses `waitForTodos`, `waitForCondition`, `subscribeAll` from existing test patterns.

### Why browser tests on top of Rust E2E?

The Rust E2E tests exercise `RuntimeCore` → `ObjectManager` → `EdgeServer` directly. The browser tests additionally exercise:

- **WASM bindings** (`jazz-wasm`) — serialization/deserialization of commits across the FFI boundary
- **Web Worker bridge** — `postMessage` channel, leader/follower election, OPFS persistence
- **Binary sync transport** — frame parsing, payload batching over WebSocket
- **Subscription delta pipeline** — `SubscriptionManager` → callback delivery on main thread

A bug in any of these layers could silently drop commits or deliver stale state, which the Rust tests wouldn't catch.

### Test 12: `concurrent_updates_converge_in_browser`

```text
dbAlice ──insert todo──► server ◄──update same todo── dbBob
         (both update title concurrently)

         waitForTodos on both → same title
```

- Alice inserts a todo via `insertDurable`, wait for Bob to see it via `waitForTodos`
- Alice calls `update(todos, id, { title: "alice-edit" })`, Bob calls `update(todos, id, { title: "bob-edit" })` — no sync wait between
- Both poll `waitForTodos` until they see the same title
- Assert convergence (identical final title on both)

### Test 13: `concurrent_creates_both_visible_in_browser`

```text
dbAlice ──insert "buy milk"──► server ◄──insert "buy eggs"── dbBob

         waitForTodos on both → see 2 todos
```

- Alice and Bob each `insertDurable` a todo concurrently
- Both `waitForTodos` → see 2 todos
- Verifies independent object creation survives the full browser pipeline

### Test 14: `subscription_fires_on_remote_concurrent_update`

```text
dbAlice subscribes via subscribeAll
dbBob updates a todo
subscription callback fires with delta containing bob's update
```

- Alice inserts a todo, subscribes via `subscribeAll`
- Bob updates the title
- Alice's subscription callback receives a delta with the updated row
- Assert via polling the snapshots array (same pattern as existing `worker-bridge.test.ts` subscription tests)

### Test 15: `fresh_db_sees_converged_state`

```text
dbAlice + dbBob conflict on a todo ──► server
                                          │
               dbCharlie connects fresh, queries
                                          │
                                          └──► sees same winner
```

- Alice and Bob create a conflict (concurrent updates to same todo)
- Wait for convergence on both via `waitForTodos`
- Create `dbCharlie` fresh via `createSyncedDb`
- Charlie's `waitForTodos` → sees same winner as Alice and Bob

## File Layout

```
crates/jazz-tools/
├── tests/
│   ├── history_conflict.rs          ← NEW: Rust e2e tests (tests 1-6)
│   ├── clients_sync.rs              ← existing
│   ├── support/mod.rs               ← existing helpers (reused)
│   └── ...
└── src/
    └── object_manager/
        └── tests.rs                 ← MODIFIED: add tests 7-11

packages/jazz-tools/
└── tests/
    └── browser/
        ├── history-conflict.test.ts ← NEW: browser tests (tests 12-15)
        ├── worker-bridge.test.ts    ← existing (reuse helpers)
        ├── testing-server.ts        ← existing (reuse)
        └── global-setup.ts          ← existing (reuse)
```

## Dependencies

- Rust E2E tests: `jazz_tools` with `test` feature, `tokio`
- Rust ObjectManager tests: `MemoryStorage`, `Commit` struct for `receive_commit`
- Browser tests: `vitest`, `@vitest/browser`, `playwright`, `jazz-wasm`, existing `testing-server-node.ts` global setup

## Future CRDT Readiness

- All E2E and browser tests assert **convergence** (both clients see same value) not specific winners
- When CRDTs arrive: add new ObjectManager tests for each merge strategy
- E2E/browser tests may need updated assertions only if semantics change (e.g., counter CRDT sums instead of LWW)
