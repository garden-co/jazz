---
name: jazz-sync
description: Design and troubleshoot Jazz synchronization, offline behavior, convergence, and conflict handling in TypeScript applications. Use for merge strategies such as lww, counter, or g-set; concurrent edits; optimistic versus edge-confirmed UI; read tiers, localUpdates, and propagation; disconnect and reconnect behavior; rejected or replayed writes; mixed-schema convergence; and deterministic multi-client conflict tests.
---

# Jazz Sync and Convergence

Design sync behavior from the product invariant backwards. Separate immediate local visibility,
authority confirmation, and eventual peer convergence instead of treating “synced” as one state.

## Inspect the installed behavior

1. Read the installed `jazz-tools` version, schema merge declarations, database configuration, and
   existing offline or multi-client tests.
2. Identify which clients can be offline, which operations may conflict, and whether duplicate
   intent or temporary divergence is acceptable.
3. Inspect current types, status-quo specs, and integration tests before relying on a remembered
   merge or reconnect guarantee. Sync behavior changes faster than ordinary query syntax.
4. Read the reference that matches the task:
   - [merge-and-convergence.md](references/merge-and-convergence.md) for `lww`, `counter`, `g-set`,
     conflict frontiers, schema-relative resolution, and data-model alternatives.
   - [propagation-and-reconnect.md](references/propagation-and-reconnect.md) for read options, write
     settlement, disconnect/reconnect, rejection, replay limitations, and deterministic tests.

## Choose the data model before the merge strategy

- Use implicit `lww` when one timestamp-ordered value should win a concurrent conflict.
- Use `counter` only on a non-nullable integer whose concurrent snapshots represent independent
  deltas from a shared ancestor.
- Use `g-set` only on a non-nullable array whose concurrent additions should converge to a
  deterministic union.
- Treat merge strategies as concurrent-frontier resolution, not validation or authorization.
- Use append-only operation rows when every intent needs identity, provenance, deduplication,
  removal semantics, or stronger exactly-once guarantees than a merged snapshot provides.
- Load `jazz-schema-evolution` when changing a merge strategy in an established schema. Mixed old
  and new clients may resolve the same history through different consumer schemas.

## Separate optimistic and confirmed state

- Keep normal product interactions local-first: writes apply locally and reactive reads may show
  them before authority settlement.
- Track the write handle when the UI must show pending, confirmed, or rejected state. Await
  `.wait({ tier: "edge" })` or `global` according to the product contract.
- Use `localUpdates: "immediate"` for a deliberately optimistic read and `"deferred"` when local
  mutations must wait behind the requested initial tier.
- Keep collaborative reads at `propagation: "full"`. Use `"local-only"` only when the read must not
  request upstream data.
- Handle rejected writes explicitly. Optimistic data can disappear when authority rejects its
  history entry; use `.wait(...)` or `db.onMutationError(...)` rather than assuming a crash.

## Treat reconnect as a correctness boundary

- Preserve stable persistent storage and identity for clients expected to work offline.
- Expect local reads and writes to continue while disconnected and stronger waits to remain pending.
- On reconnect, assert both upload of offline writes and retrieval of missed remote changes.
- Do not promise exactly-once replay without verifying the installed transport. If an accepted
  payload can be replayed after a lost acknowledgement, model non-idempotent intent with stable
  operation IDs and deduplication.

## Test real concurrency

1. Establish and confirm a shared ancestor before disconnecting or blocking propagation.
2. Prevent the second writer from observing the first before it writes; `Promise.all` alone does
   not prove a concurrent frontier.
3. Exercise both propagation or reconnect orders.
4. Await settlement and use retrying assertions for eventual delivery.
5. Assert convergence on both original clients and a fresh third client.
6. Keep result order deterministic and clean up every client, server, and subscription.

## Cross into adjacent work deliberately

- Load `jazz-core` when implementing ordinary framework queries, writes, or UI around the sync
  design.
- Load `jazz-testing` when the requested work includes TypeScript sync test code.
- Load `jazz-rust` for native Rust convergence work or Rust integration tests.

## Avoid these failure modes

- Do not call a write remotely durable merely because the initiating client rendered it.
- Do not treat `g-set` as an unconditional no-removal rule across sequential history.
- Do not assume a counter API is an atomic increment command; confirm its snapshot semantics.
- Do not use fixed sleeps or scheduler races to manufacture conflict tests.
- Do not infer exactly-once delivery from eventual convergence of idempotent values.
- Do not roll out a merge-strategy change without considering old clients.
