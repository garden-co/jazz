# Task: batch edge-tier writes in the sync transport worker

## Context

The Jazz client sends one HTTP POST per edge-tier write. When application code calls
`db.update(..., { tier: "edge" })` at high frequency (e.g. 60fps game loop updating a
player position row), the worker fans each WASM outbox payload out to a separate
`sendToServer()` call — one HTTP POST each. This floods the Jazz server and causes stream
connect timeouts for other connected clients.

### Hot path (traced from db.update to the network)

```
db.update(table, id, data, { tier: "edge" })          packages/jazz-tools/src/runtime/db.ts
  → client.updateDurable(id, updates, "edge")          packages/jazz-tools/src/runtime/client.ts
  → WASM runtime.updateDurable()                        [binary from here — do not coalesce above WASM]
  → outbox callback → WorkerBridge.enqueue()            packages/jazz-tools/src/runtime/worker-bridge.ts
      already microtask-batches to worker via queueMicrotask
  → worker.postMessage([...payloads])
  → for (payload of payloads):                          packages/jazz-tools/src/worker/jazz-worker.ts
      runtime.onSyncMessageReceivedFromClient(payload)
      → sendToServer(payload)                           ← one HTTP POST per payload — THIS IS THE PROBLEM
```

`WorkerBridge` already batches the main-thread → worker `postMessage` on a microtask
boundary. The gap is that the worker still fans each payload out to a separate HTTP POST.

### Why write coalescing (merge/dedup) is wrong

Merging multiple writes to the same row loses intermediate states. If `collected` goes
`false → true → false` in one tick and you coalesce to `false`, the server never fires
WHERE ENTRY for the `true` state — remote clients miss the event entirely. All writes must
reach the server in order.

### Correct fix: ordered transport batching in the worker

Keep every write intact and in order. Accumulate payloads in the worker over a microtask
boundary, then send them as an **ordered array in a single POST**. The server applies them
sequentially. No intermediate states are lost; the only change is fewer HTTP round-trips.

## Design decisions

### Wire format: always-array on the existing `/sync` endpoint

The `/sync` endpoint wire format changes to always use an array body:

```json
{"payloads": ["<payload-json>", ...], "client_id": "<id>"}
```

The old single-payload format (`{"payload": ..., "client_id": ...}`) is dropped entirely.
Even a single payload becomes a one-element array. This gives the server a single code path
with no union-type branching, and no new route. Backcompat break is acceptable (pre-launch).

### Catalogue payloads bypass the batch queue

Catalogue payloads are rare (schema/lens sync) and already use a separate auth header
(`X-Jazz-Admin-Secret`). They are not batched — they continue to be sent immediately on
their existing path, unchanged.

### Per-payload failure responses

The server applies payloads sequentially and returns a result for each:

```json
{"results": [{"ok": true}, {"ok": false, "error": "..."}, ...]}
```

Results are in the same order as the input `payloads` array. The client iterates results; on
any `ok: false` it logs the error then calls `detachServer()` + `scheduleReconnect()` (same
blast radius as today), but now has per-payload visibility for debugging.

## Changes needed

1. **Worker side** (`packages/jazz-tools/src/worker/jazz-worker.ts`): instead of calling
   `sendToServer(payload)` immediately for each server-bound, non-catalogue payload, push to
   a pending queue and flush via `queueMicrotask`. The flush sends all pending payloads as a
   single batched request.

2. **Transport side** (`packages/jazz-tools/src/runtime/sync-transport.ts`): replace
   `sendSyncPayload` (single) with `sendSyncPayloadBatch(serverUrl, payloads[], auth, ...)`
   that sends the always-array body `{"payloads":[...],"client_id":"..."}` in a single POST.

3. **Server side** (`crates/jazz-tools/src/routes.rs` and `transport_protocol.rs`): change
   `SyncPayloadRequest` to hold `payloads: Vec<SyncPayload>` and update `sync_handler` to
   apply each payload in order, collecting per-payload results into the new response shape.

Two test suites:

### 1. Rust — server receiving batches

Location: `crates/jazz-tools/src/routes.rs` (inline `#[cfg(test)]` module)

Test that:

- A batch of N payloads is applied in order and returns N `ok: true` results.
- A batch with one bad payload returns `ok: false` for that entry (and processes the rest).
- Auth is enforced on the whole request (one auth check per POST, not per payload).

### 2. TypeScript — worker sending batches

Location: `packages/jazz-tools/src/worker/jazz-worker.test.ts` (new file)

Test that:

- N server-bound payloads enqueued synchronously are flushed in a single call to
  `sendSyncPayloadBatch` after a microtask boundary, with payloads in the original order.
- Catalogue payloads are not included in the batch queue.
- A second wave of payloads enqueued after the first flush produces a second batch (not
  merged with the first).

Existing test files to look at for patterns:

- `packages/jazz-tools/src/runtime/sync-transport.test.ts`
- `packages/jazz-tools/src/runtime/worker-bridge.test.ts`
- `packages/jazz-tools/tests/browser/worker-bridge.test.ts`

## Constraints

- Do not coalesce/merge write payloads — order and completeness must be preserved.
- Worker-tier writes (local OPFS only) are unaffected — only server-bound, non-catalogue
  payloads are batched.
- The fix should be invisible to callers — no public API changes.
